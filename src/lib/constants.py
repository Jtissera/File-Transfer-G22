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

# Protocolos de transferencia
PROTOCOL_SW = 0x01  # Stop & Wait
PROTOCOL_GBN = 0x02  # Go-Back-N

MSG_NAMES = {
    MSG_UPLOAD_REQ: "UPLOAD_REQ",
    MSG_DOWNLOAD_REQ: "DOWNLOAD_REQ",
    MSG_DATA: "DATA",
    MSG_ACK: "ACK",
    MSG_ERROR: "ERROR",
}

# Valores utilizados para el calculo del rtt
ALPHA = 0.125
BETA = 0.25

# Configuración del control de timeouts:
DEFAULT_TIMEOUT = 0.2  # timeout inicial
MAX_RETRIES = 100  # maximo de reintentos antes de abortar
MAX_RTO = 3.0  # valor maximo que puede alcanzar el timeout

# Configuración de la ventana de GBN
WINDOW_INITIAL_SIZE = 1
WINDOW_MAX_SIZE = 32
