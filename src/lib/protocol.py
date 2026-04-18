import struct
import zlib

# Tamaño del header
HEADER_SIZE = 16

# Tamaño máximo de carga útil por paquete (seguro para UDP sobre Ethernet)
# Usamos 1400 para tener un margen extra
MAX_PAYLOAD_SIZE = 1400
MAX_PACKET_SIZE = HEADER_SIZE + MAX_PAYLOAD_SIZE

# Tipos de mensaje
MSG_UPLOAD_REQ = 0x01  # client a server: empieza la carga
MSG_DOWNLOAD_REQ = 0x02  # client a server: empieza la descarga
MSG_DATA = 0x03  # fragmento del archivo
MSG_ACK = 0x04  # confirma secuencia
MSG_ERROR = 0x05  # notificación de error

MSG_NAMES = {
    MSG_UPLOAD_REQ: "UPLOAD_REQ",
    MSG_DOWNLOAD_REQ: "DOWNLOAD_REQ",
    MSG_DATA: "DATA",
    MSG_ACK: "ACK",
    MSG_ERROR: "ERROR",
}


# Header format:


# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |     type      |    padding   |           checksum             |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                         seq_number                            |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |                         ack_number                            |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
# |           payload_len         |          padding              |
# +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

# B  = msg_type    (1 byte)
# B  = padding     (1 byte) alignment
# H  = checksum    (2 bytes)
# I  = seq_number  (4 bytes)
# I  = ack_number  (4 bytes)
# H  = payload_len (2 bytes)
# H  = padding     (2 bytes) alignment

# B Unsigned char
# H Unsigned short
# I Unsigned int

# Agrego esos padding para alinear el struct, mejorando la eficiencia y la facil lectura.

HEADER_FORMAT = "!BBHIIHH"


def compute_checksum(data: bytes) -> int:
    """Realiza un checksum CRC32 truncado a 16 bits."""
    return zlib.crc32(data) & 0xFFFF


def build_packet(
    msg_type: int, seq_number: int, ack_number: int, payload: bytes = b""
) -> bytes:
    """
    Crea el paquete listo para ser enviado.
    """
    payload_len = len(payload)

    header_no_checksum = struct.pack(
        HEADER_FORMAT,
        msg_type,
        0,
        0,
        seq_number,
        ack_number,
        payload_len,
        0,
    )
    checksum = compute_checksum(header_no_checksum + payload)

    header = struct.pack(
        HEADER_FORMAT, msg_type, 0, checksum, seq_number, ack_number, payload_len, 0
    )
    return header + payload


def parse_packet(raw: bytes) -> dict:
    """
    Parsea un paquete a un diccionario.
    Si el checksum del paquete no coincide, se lanza un ValueError.
    """

    (
        msg_type,
        _,
        received_checksum,
        seq_number,
        ack_number,
        payload_len,
        _,
    ) = struct.unpack(HEADER_FORMAT, raw[:HEADER_SIZE])

    payload = raw[HEADER_SIZE:]

    if len(payload) != payload_len:
        raise ValueError(
            f"El largo del payload no coincide: "
            f"Encabezado: {payload_len} bytes, obtenido: {len(payload)}"
        )

    # Recompute checksum with the field zeroed out
    header_for_check = struct.pack(
        HEADER_FORMAT, msg_type, 0, 0, seq_number, ack_number, payload_len, 0
    )
    expected = compute_checksum(header_for_check + payload)

    if received_checksum != expected:
        raise ValueError(
            f"Checksum no coincide: "
            f"esperado: {expected:#06x}, obtenido: {received_checksum:#06x}"
        )

    return {
        "type": msg_type,
        "type_name": MSG_NAMES.get(msg_type, f"UNKNOWN({msg_type:#04x})"),
        "seq_number": seq_number,
        "ack_number": ack_number,
        "checksum": received_checksum,
        "payload_len": payload_len,
        "payload": payload,
    }


# ---------------------------------------------------------------------------
# Flujo de carga
#
# El cliente envía UPLOAD_REQ con el nombre de archivo + tamaño de archivo en el payload.
# El servidor responde con ACK(0) para confirmar que está listo.
# El cliente envía DATA hasta que se envían todos los bytes.
# El servidor envía ACK(N) después de cada paquete DATA que acepta.
# El servidor sabe que la transferencia se ha completado cuando los bytes recibidos == tamaño del archivo.
# ---------------------------------------------------------------------------


def build_upload_req(filename: str, filesize: int) -> bytes:
    """
    Crea un paquete UPLOAD_REQ.
    En el payload se incluyen 2 bytes para el largo del nombre de archivo,
    N bytes para el nombre de archivo (UTF-8) y
    4 bytes para el tamaño del archivo.
    """
    name_bytes = filename.encode("utf-8")
    payload = (
        struct.pack("!H", len(name_bytes)) + name_bytes + struct.pack("!I", filesize)
    )
    return build_packet(MSG_UPLOAD_REQ, payload=payload)


def parse_upload_req(payload: bytes) -> tuple:
    """
    Desempaqueta el payload de un paquete UPLOAD_REQ.
    Devuelve una tupla (filename: str, filesize: int).
    """
    name_len = struct.unpack("!H", payload[:2])[0]
    filename = payload[2 : 2 + name_len].decode("utf-8")
    filesize = struct.unpack("!I", payload[2 + name_len : 2 + name_len + 4])[0]
    return filename, filesize


# ---------------------------------------------------------------------------
# Flujo de descarga
#
# El cliente envía DOWNLOAD_REQ con el nombre de archivo en el payload.
# El servidor responde con DATA cuyo payload comienza con el tamaño del archivo
# (4 bytes) seguido del primer fragmento de datos del archivo.
# El cliente sabe que la transferencia se ha completado cuando los bytes recibidos == tamaño del archivo.
# ---------------------------------------------------------------------------


def build_download_req(filename: str) -> bytes:
    """
    Crea un paquete DOWNLOAD_REQ.
    En el payload se incluyen 2 bytes para el largo del nombre de archivo,
    N bytes para el nombre de archivo (UTF-8).
    """
    name_bytes = filename.encode("utf-8")
    payload = struct.pack("!H", len(name_bytes)) + name_bytes
    return build_packet(MSG_DOWNLOAD_REQ, payload=payload)


def parse_download_req(payload: bytes) -> str:
    """
    Desempaqueta el payload de un paquete DOWNLOAD_REQ.
    Devuelve el nombre de archivo: str.
    """
    name_len = struct.unpack("!H", payload[:2])[0]
    return payload[2 : 2 + name_len].decode("utf-8")


def build_first_data(seq: int, filesize: int, chunk: bytes) -> bytes:
    """
    Crea el primer paquete DATA en una respuesta de descarga.
    En el payload se incluyen:

      4 bytes  -- tamaño del archivo (unsigned int)
      N bytes  -- primer fragmento de datos del archivo
    """
    payload = struct.pack("!I", filesize) + chunk
    return build_packet(MSG_DATA, seq_number=seq, payload=payload)


def parse_first_data(payload: bytes) -> tuple:
    """
    Desempaqueta el payload del primer paquete DATA en una respuesta de descarga.
    Devuelve una tupla (filesize: int, chunk: bytes).
    """
    filesize = struct.unpack("!I", payload[:4])[0]
    return filesize, payload[4:]


def build_data(seq: int, chunk: bytes) -> bytes:
    return build_packet(MSG_DATA, seq_number=seq, payload=chunk)


def build_ack(ack: int) -> bytes:
    return build_packet(MSG_ACK, ack_number=ack)


def build_error(message: str) -> bytes:
    return build_packet(MSG_ERROR, payload=message.encode("utf-8"))


def parse_error(payload: bytes) -> str:
    return payload.decode("utf-8")
