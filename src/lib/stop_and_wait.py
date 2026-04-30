"""
Stop & Wait — módulo de transferencia confiable sobre UDP.

Provee dos funciones principales:
  - send():envia datos de forma confiable usando stop & wait
  - receive(): recibe datos de forma confiable usando stop & wait

Funcionamiento:
  1. El sender envía un paquete DATA con seq_number = N
  2. Espera un ACK con ack_number = N
  3. Si recibe el ACK correcto, avanza al siguiente paquete (N+1)
  4. Si hay timeout, reenvía el mismo paquete (hasta max_retries veces)
  5. El receiver envía ACK al recibir cada DATA con el seq esperado
  6. Si recibe un DATA duplicado (seq < esperado),
     reenvía el ACK correspondiente
"""

import socket as _socket
import time

from rtt import (
    rtt_update,
    rtt_duplicate,
    rtt_new,
    rtt_timeout,
)

from protocol import (
    build_data,
    build_ack,
    parse_packet,
)

from constants import (
    MSG_DATA,
    MSG_ACK,
    MSG_ERROR,
    MAX_PAYLOAD_SIZE,
    MAX_PACKET_SIZE,
    DEFAULT_TIMEOUT,
    MAX_RETRIES,
)


class TransferError(Exception):
    """Error durante la transferencia de datos."""

    pass


def send(
    sock,
    addr,
    data,
    first_seq=0,
    logger=None,
    initial_rtt=None,
    max_retries=MAX_RETRIES,
):
    """
    Envia ``data``  usando stop & wait.

    divide los datos en chunks de MAX_PAYLOAD_SIZE y envia cada uno
    esperando el ACK correspondiente antes de pasar al siguiente.

    Args:
        sock: socket UDP ya creado
        addr: tupla (host, port) del destinatario
        data: bytes a enviar
        first_seq: número de secuencia inicial (default 0)
        logger: logger (opcional)
        initial_rtt: estimación inicial del round trip time
        max_retries: reintentos máximos por paquete

    Raises:
        TransferError: si se superan los reintentos para algún paquete
    """
    total = len(data)
    offset = 0
    seq = first_seq

    if initial_rtt is None:
        rtt = rtt_new()
    else:
        rtt = initial_rtt

    while offset < total:
        chunk = data[offset : offset + MAX_PAYLOAD_SIZE]
        packet = build_data(seq, chunk)

        ack_received = False
        retries = 0
        rtt_pkt = dict(rtt)

        while not ack_received:
            if retries >= max_retries:
                raise TransferError(
                    f"Se superaron los {max_retries} reintentos para seq={seq}"
                )

            sock.sendto(packet, addr)
            send_time = time.time()
            if logger:
                logger.debug(
                    f"[S&W] Enviado DATA seq={seq} "
                    f"({offset + len(chunk)}/{total} bytes)"
                )

            try:
                sock.settimeout(rtt_timeout(rtt_pkt))
                raw, _ = sock.recvfrom(MAX_PACKET_SIZE)
                pkt = parse_packet(raw)

                if pkt["type"] == MSG_ACK and pkt["ack_number"] == seq:
                    ack_received = True

                    if retries == 0:
                        rtt = rtt_update(rtt, time.time() - send_time)
                    if logger:
                        logger.debug(f"[S&W] Recibido ACK ack={seq}")

                elif pkt["type"] == MSG_ERROR:
                    error_msg = pkt["payload"].decode(
                        "utf-8", errors="replace"
                    )
                    raise TransferError(f"Error del receptor: {error_msg}")

                else:
                    if logger:
                        logger.debug(
                            f"[S&W] Paquete inesperado: "
                            f"type={pkt['type_name']} ack={pkt['ack_number']}"
                        )
                    retries += 1

            except _socket.timeout:
                retries += 1
                rtt_pkt = rtt_duplicate(rtt_pkt)
                if logger:
                    logger.debug(
                        f"[S&W] Timeout seq={seq}, "
                        f"reintento {retries}/{max_retries}"
                    )

            except ValueError as e:
                # checksum invalido, descarta paquete y reintenta
                retries += 1
                if logger:
                    logger.debug(f"[S&W] Paquete corrupto descartado: {e}")

        offset += len(chunk)
        seq += 1

    if logger:
        logger.info(f"[S&W] Transferencia completada: {total} bytes enviados")


def receive(
    sock,
    filepath,
    expected_bytes,
    first_seq=0,
    logger=None,
):
    """
    Recibe datos de forma confiable usando Stop & Wait.

    Espera paquetes DATA en orden secuencial, respondiendo con ACK
    por cada uno recibido correctamente.

    Args:
        sock:           socket UDP ya creado
        filepath:       ruta del archivo a escribir
        expected_bytes: cantidad total de bytes esperados
        first_seq:      número de secuencia inicial esperado (default 0)
        logger:         logger (opcional)

    Raises:
        TransferError: si hay demasiados timeouts consecutivos
    """
    bytes_received = 0
    expected_seq = first_seq
    sender_addr = None
    last_ack_sent = first_seq - 1
    sock.settimeout(None)

    try:
        with open(filepath, "xb") as f:
            if logger:
                logger.debug(f"[S&W] Se creo el archivo en {filepath}")

    except FileExistsError:
        raise TransferError(f'[S&W] El archivo "{filepath}" ya existe')

    except OSError as e:
        raise TransferError(
            f'[S&W] No se pudo crear el archivo "{filepath}": {e}'
        )

    while bytes_received < expected_bytes:
        raw, addr = sock.recvfrom(MAX_PACKET_SIZE)

        try:
            pkt = parse_packet(raw)
        except ValueError as e:
            if logger:
                logger.debug(f"[S&W] Paquete corrupto descartado: {e}")
            continue

        if sender_addr is not None and addr != sender_addr:
            if logger:
                logger.debug(
                    f"[S&W] Paquete de {addr} ignorado"
                    f" (esperaba {sender_addr})"
                )
            continue

        if pkt["type"] == MSG_ERROR:
            error_msg = pkt["payload"].decode("utf-8", errors="replace")
            raise TransferError(f"Error del emisor: {error_msg}")

        if pkt["type"] != MSG_DATA:
            if logger:
                logger.debug(
                    f"[S&W] Esperaba DATA, recibido {pkt['type_name']}"
                )
            continue

        if sender_addr is None:
            sender_addr = addr

        if pkt["seq_number"] == expected_seq:

            with open(filepath, "ab") as f:
                f.write(pkt["payload"])

            bytes_received += len(pkt["payload"])
            ack = build_ack(expected_seq)
            sock.sendto(ack, addr)
            last_ack_sent = expected_seq

            if logger:
                logger.debug(
                    f"[S&W] Recibido DATA seq={expected_seq} "
                    f"({bytes_received}/{expected_bytes} bytes), ACK enviado"
                )
            expected_seq += 1

        elif pkt["seq_number"] < expected_seq:
            ack = build_ack(pkt["seq_number"])
            sock.sendto(ack, addr)
            if logger:
                logger.debug(
                    f"[S&W] Duplicado seq={pkt['seq_number']}, "
                    f"reenviado ACK"
                )

        else:
            # Paquete futuro (no debería ocurrir)

            if last_ack_sent >= first_seq:
                ack = build_ack(last_ack_sent)
                sock.sendto(ack, addr)

            if logger:
                logger.debug(
                    f"[S&W] seq={pkt['seq_number']} fuera de orden, "
                    f"esperaba {expected_seq}"
                )

    if logger:
        logger.info(
            f"[S&W] Recepción completada: {bytes_received} bytes recibidos"
        )

    final_ack_retries = MAX_RETRIES

    while (
        final_ack_retries > 0
        and sender_addr is not None
        and last_ack_sent >= first_seq
    ):
        try:
            sock.settimeout(DEFAULT_TIMEOUT)
            raw, addr = sock.recvfrom(MAX_PACKET_SIZE)

            if addr != sender_addr:
                continue

            try:
                pkt = parse_packet(raw)
            except ValueError:
                continue

            if pkt["type"] == MSG_DATA:
                ack = build_ack(last_ack_sent)
                sock.sendto(ack, addr)

                if logger:
                    logger.debug(
                        f"[GBN] Linger: duplicado/fuera de orden "
                        f"seq={pkt['seq_number']}, "
                        f"reenviado ACK={last_ack_sent}"
                    )

        except _socket.timeout:
            final_ack_retries -= 1
