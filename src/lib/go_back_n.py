
"""
Go-Back-N — módulo de transferencia confiable sobre UDP.


Provee dos funciones principales:
 - send(): envía datos de forma confiable usando Go-Back-N
 - receive(): recibe datos de forma confiable usando Go-Back-N


Funcionamiento del sender:
 1. Mantiene una ventana [base, next_seq)
 2. Envía paquetes mientras haya lugar en la ventana
 3. Espera ACKs acumulativos
 4. Si recibe ACK(N), avanza base a N+1
 5. Si hay timeout del paquete base, retransmite toda la ventana


Funcionamiento del receiver:
 1. Acepta solamente el paquete con seq esperado
 2. Si llega en orden, lo guarda y envía ACK acumulativo
 3. Si llega fuera de orden o duplicado, reenvía el último ACK válido


Aclaracion:
 El rtt solo se actualiza en caso de que no haya ocurrido una retransmicion ya que si
 ocurre un timeout y se reenvia, cuando llegue el ACK no tengo ni idea de si le pertenece
 al paquete original (solo tardo un poco mas) o si es del retransmitido => no sabes que sample_rtt
 deberias pasarle.
"""


import socket as _socket
from rtt import *
import time


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
   MAX_RETRIES,
)


WINDOW_SIZE = 8


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
   window_size=WINDOW_SIZE,
):
   """
   Envía ``data`` usando Go-Back-N.


   Divide los datos en chunks de MAX_PAYLOAD_SIZE. Mantiene una ventana
   deslizante y procesa ACKs acumulativos. Ante timeout retransmite todos
   los paquetes pendientes de ACK dentro de la ventana.


   Args:
       sock: socket UDP ya creado
       addr: tupla (host, port) del destinatario
       data: bytes a enviar
       first_seq: número de secuencia inicial
       logger: logger opcional
       timeout: segundos de espera por ACK antes de retransmitir
       max_retries: máximo de timeouts/retransmisiones de la ventana base
       window_size: tamaño de ventana


   Returns:
       int: siguiente número de secuencia disponible


   Raises:
       TransferError: si se superan los reintentos permitidos
   """
   total_bytes = len(data)
   if total_bytes == 0:
       if logger:
           logger.info("[GBN] Transferencia completada: 0 bytes enviados")
       return first_seq


   chunks = [data[i : i + MAX_PAYLOAD_SIZE] for i in range(0, total_bytes, MAX_PAYLOAD_SIZE)]
   total_packets = len(chunks)


   base_idx = 0
   next_idx = 0
   retries = 0


      
   if initial_rtt == None:
       rtt = rtt_new()
   else:
       rtt = initial_rtt


   send_times = {}
   rtt_pkt = dict(rtt)


   while base_idx < total_packets:
       while next_idx < total_packets and next_idx < base_idx + window_size:
           seq = first_seq + next_idx
           packet = build_data(seq, chunks[next_idx])
           sock.sendto(packet, addr)
           send_times[seq] = time.time()


           if logger:
               sent_bytes = min((next_idx + 1) * MAX_PAYLOAD_SIZE, total_bytes)
               logger.debug(
                   f"[GBN] Enviado DATA seq={seq} "
                   f"(ventana={first_seq + base_idx}-{first_seq + min(base_idx + window_size - 1, total_packets - 1)}) "
                   f"({sent_bytes}/{total_bytes} bytes)"
               )


           next_idx += 1


       try:

           sock.settimeout(rtt_timeout(rtt_pkt))
           raw, _recv_addr = sock.recvfrom(MAX_PACKET_SIZE)
           pkt = parse_packet(raw)


           if pkt["type"] == MSG_ACK:
               ack_number = pkt["ack_number"]
               last_valid_seq = first_seq + total_packets - 1


               if ack_number < first_seq - 1 or ack_number > last_valid_seq:
                   if logger:
                       logger.debug(f"[GBN] ACK fuera de rango ack={ack_number}, ignorado")
                   continue


               new_base_idx = (ack_number - first_seq) + 1
               if new_base_idx > base_idx:
                   if logger:
                       logger.debug(
                           f"[GBN] Recibido ACK acumulativo ack={ack_number}, "
                           f"base avanza de seq={first_seq + base_idx} "
                           f"a seq={first_seq + new_base_idx if new_base_idx < total_packets else 'FIN'}"
                       )

                        
                  
                   if retries == 0 and ack_number in send_times:
                        
                       rtt = rtt_update(rtt, time.time() - send_times[ack_number])


                   base_idx = new_base_idx
                   retries = 0
                   rtt_pkt = dict(rtt)
              
               else:
                   if logger:
                       logger.debug(f"[GBN] ACK duplicado ack={ack_number}")


           elif pkt["type"] == MSG_ERROR:
               error_msg = pkt["payload"].decode("utf-8", errors="replace")
               raise TransferError(f"Error del receptor: {error_msg}")


           else:
               if logger:
                   logger.debug(
                       f"[GBN] Paquete inesperado: "
                       f"type={pkt['type_name']} ack={pkt['ack_number']}"
                   )


       except _socket.timeout:
           retries += 1
           rtt_pkt =rtt_duplicate(rtt_pkt)
          
           if retries >= max_retries:
               raise TransferError(
                   f"Se superaron los {max_retries} timeouts/reintentos "
                   f"para la ventana base seq={first_seq + base_idx}"
               )


           if logger:
               logger.debug(
                   f"[GBN] Timeout esperando ACK para base seq={first_seq + base_idx}, "
                   f"retransmitiendo ventana ({retries}/{max_retries})"
               )


           for idx in range(base_idx, next_idx):
               seq = first_seq + idx
               packet = build_data(seq, chunks[idx])
               sock.sendto(packet, addr)
               send_times[seq] = time.time()
               if logger:
                   logger.debug(f"[GBN] Retransmitido DATA seq={seq}")


       except ValueError as e:
           if logger:
               logger.debug(f"[GBN] Paquete corrupto descartado: {e}")


   if logger:
       logger.info(f"[GBN] Transferencia completada: {total_bytes} bytes enviados")


   return first_seq + total_packets


def receive(
   sock,
   expected_bytes,
   first_seq=0,
   logger=None,
   timeout=DEFAULT_TIMEOUT,
   max_retries=MAX_RETRIES,
):
   """
   Recibe datos de forma confiable usando Go-Back-N.


   Acepta únicamente el siguiente número de secuencia esperado. Cuando llega
   un paquete fuera de orden, lo descarta y reenvía el último ACK válido.
   """
   data = bytearray()
   expected_seq = first_seq
   sender_addr = None
   consecutive_timeouts = 0
   last_ack_sent = first_seq - 1


   while len(data) < expected_bytes:

       raw, addr = sock.recvfrom(MAX_PACKET_SIZE)

       try:
           pkt = parse_packet(raw)
       except ValueError as e:
           if logger:
               logger.debug(f"[GBN] Paquete corrupto descartado: {e}")
           continue


       if sender_addr is not None and addr != sender_addr:
           if logger:
               logger.debug(f"[GBN] Paquete de {addr} ignorado (esperaba {sender_addr})")
           continue


       if pkt["type"] == MSG_ERROR:
           error_msg = pkt["payload"].decode("utf-8", errors="replace")
           raise TransferError(f"Error del emisor: {error_msg}")


       if pkt["type"] != MSG_DATA:
           if logger:
               logger.debug(f"[GBN] Esperaba DATA, recibido {pkt['type_name']}")
           continue


       if sender_addr is None:
           sender_addr = addr


       seq = pkt["seq_number"]


       if seq == expected_seq:
           remaining = expected_bytes - len(data)
           payload = pkt["payload"][:remaining]
           data.extend(payload)


           ack = build_ack(seq)
           sock.sendto(ack, addr)
           last_ack_sent = seq


           if logger:
               logger.debug(
                   f"[GBN] Recibido DATA seq={seq} "
                   f"({len(data)}/{expected_bytes} bytes), ACK acumulativo enviado"
               )


           expected_seq += 1


       else:
           if last_ack_sent >= first_seq:
               ack = build_ack(last_ack_sent)
               sock.sendto(ack, addr)
               if logger:
                   logger.debug(
                       f"[GBN] DATA fuera de orden/duplicado seq={seq}, "
                       f"esperaba {expected_seq}; reenviado ACK={last_ack_sent}"
                   )
           else:
               if logger:
                   logger.debug(
                       f"[GBN] DATA fuera de orden seq={seq}, "
                       f"esperaba {expected_seq}; aún no hay ACK válido para reenviar"
                   )


   if logger:
       logger.info(f"[GBN] Recepción completada: {len(data)} bytes recibidos")


   final_ack_retries = 5


   while final_ack_retries > 0 and sender_addr is not None and last_ack_sent >= first_seq:
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
                       f"[GBN] Linger: duplicado/fuera de orden seq={pkt['seq_number']}, "
                       f"reenviado ACK={last_ack_sent}"
                   )


       except _socket.timeout:
           final_ack_retries -= 1






   return bytes(data), sender_addr
