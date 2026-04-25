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
  6. Si recibe un DATA duplicado (seq < esperado), reenvía el ACK correspondiente

  Aclaracion:
  El rtt solo se actualiza en caso de que no haya ocurrido una retransmicion ya que si
  ocurre un timeout y se reenvia, cuando llegue el ACK no tengo ni idea de si le pertenece
  al paquete original (solo tardo un poco mas) o si es del retransmitido => no sabes que sample_rtt
  deberias pasarle.
"""

import socket as _socket
import time

from rtt import *

from protocol import (
    build_data,
    build_ack,
    parse_packet,
    MSG_DATA,
    MSG_ACK,
    MSG_ERROR,
    MAX_PAYLOAD_SIZE,
    MAX_PACKET_SIZE,
    DEFAULT_TIMEOUT,
    MAX_RETRIES
)




class TransferError(Exception):
    """Error durante la transferencia de datos."""

    pass



# Sender



def send(
    sock,
    addr,
    data,
    first_seq=0,
    logger=None,
    initial_rtt = None,
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
        timeout: segundos de espera por ACK antes de reenviar
        max_retries: reintentos máximos por paquete

    Returns:
        int: siguiente número de secuencia disponible

    Raises:
        TransferError: si se superan los reintentos para algún paquete
    """
    total = len(data)
    offset = 0
    seq = first_seq

    if initial_rtt == None:
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

            # Enviar paquete DATA
            sock.sendto(packet, addr)
            send_time = time.time() # tiempo actual en seg
            if logger:
                logger.debug(
                    f"[S&W] Enviado DATA seq={seq} "
                    f"({offset + len(chunk)}/{total} bytes)"
                )

            # Esperar ACK
            try:
                sock.settimeout(rtt_timeout(rtt_pkt))
                raw, _recv_addr = sock.recvfrom(MAX_PACKET_SIZE)
                pkt = parse_packet(raw)

                if pkt["type"] == MSG_ACK and pkt["ack_number"] == seq:
                    ack_received = True
                    if retries == 0:
                        rtt = rtt_update(rtt, time.time() - send_time) # La resta es a lo que refiere la aclaracion del incio
                    if logger:
                        logger.debug(f"[S&W] Recibido ACK ack={seq}")

                elif pkt["type"] == MSG_ERROR:
                    error_msg = pkt["payload"].decode("utf-8", errors="replace")
                    raise TransferError(f"Error del receptor: {error_msg}")

                else:
                    # ACK de otro seq o paquete inesperado, ignorar y reintentar
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

    return seq



# Receiver



def receive(
    sock,
    expected_bytes,
    first_seq=0,
    logger=None,
    max_retries=MAX_RETRIES,
):
    """
    Recibe datos de forma confiable usando Stop & Wait.

    Espera paquetes DATA en orden secuencial, respondiendo con ACK
    por cada uno recibido correctamente.

    Args:
        sock:           socket UDP ya creado
        expected_bytes: cantidad total de bytes esperados
        first_seq:      número de secuencia inicial esperado (default 0)
        logger:         logger (opcional)
        timeout:        segundos de espera por cada DATA
        max_retries:    máximo de timeouts consecutivos sin recibir datos

    Returns:
        tuple: (data: bytes, sender_addr: tuple)

    Raises:
        TransferError: si hay demasiados timeouts consecutivos
    """
    data = bytearray()
    expected_seq = first_seq
    sender_addr = None

    while len(data) < expected_bytes:

        raw, addr = sock.recvfrom(MAX_PACKET_SIZE)


        # Validar integridad del paquete
        try:
            pkt = parse_packet(raw)
        except ValueError as e:
            if logger:
                logger.debug(f"[S&W] Paquete corrupto descartado: {e}")
            continue

        # Filtrar por sender (una vez establecido)
        if sender_addr is not None and addr != sender_addr:
            if logger:
                logger.debug(f"[S&W] Paquete de {addr} ignorado (esperaba {sender_addr})")
            continue

        # Manejar paquete de error
        if pkt["type"] == MSG_ERROR:
            error_msg = pkt["payload"].decode("utf-8", errors="replace")
            raise TransferError(f"Error del emisor: {error_msg}")

        # Ignorar paquetes que no sean DATA
        if pkt["type"] != MSG_DATA:
            if logger:
                logger.debug(f"[S&W] Esperaba DATA, recibido {pkt['type_name']}")
            continue

        # Registrar dirección del sender con el primer DATA recibido
        if sender_addr is None:
            sender_addr = addr

        if pkt["seq_number"] == expected_seq:
            # Paquete esperado: guardar datos y enviar ACK
            data.extend(pkt["payload"])
            ack = build_ack(expected_seq)
            sock.sendto(ack, addr)

            if logger:
                logger.debug(
                    f"[S&W] Recibido DATA seq={expected_seq} "
                    f"({len(data)}/{expected_bytes} bytes), ACK enviado"
                )
            expected_seq += 1

        elif pkt["seq_number"] < expected_seq:
            # Duplicado: reenviar el ACK para que el sender avance
            ack = build_ack(pkt["seq_number"])
            sock.sendto(ack, addr)
            if logger:
                logger.debug(
                    f"[S&W] Duplicado seq={pkt['seq_number']}, "
                    f"reenviado ACK"
                )

        else:
            # Paquete futuro (no debería ocurrir)
            if logger:
                logger.debug(
                    f"[S&W] seq={pkt['seq_number']} fuera de orden, "
                    f"esperaba {expected_seq}"
                )

    if logger:
        logger.info(f"[S&W] Recepción completada: {len(data)} bytes recibidos")

    return bytes(data), sender_addr