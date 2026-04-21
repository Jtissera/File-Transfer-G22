# File Transfer UDP

## Descripción

Este proyecto implementa una aplicación de transferencia de archivos sobre UDP utilizando dos protocolos de recuperación de errores:

- Stop & Wait (S&W)  
- Go-Back-N (GBN)  

La aplicación permite realizar:

- **UPLOAD**: envío de un archivo desde el cliente al servidor  
- **DOWNLOAD**: descarga de un archivo desde el servidor  

Se implementan mecanismos de confiabilidad como numeración de secuencia, ACKs, retransmisiones y manejo de timeouts.

---

## Requisitos

- Python 3  
- Mininet (para pruebas de red)  
- Sistema Linux recomendado  

---

## Estructura del proyecto

```
FILE-TRANSFER-G22/
├── src/
│ ├── lib/
│ │ ├── go_back_n.py
│ │ ├── stop_and_wait.py
│ │ ├── protocol.py
│ │ └── logger.py
│ ├── storage/
│ ├── upload
│ ├── download
│ ├── start-server
├── informe.pdf
└── README.md
```

---

## Cómo ejecutar

### 1. Levantar el servidor

```bash
python3 start-server -H <host> -p <port> -s <storage_dir>
```

Ejemplo:

```bash
python3 start-server -H 127.0.0.1 -p 9000 -s storage
```

---

### 2. Subir un archivo (UPLOAD)

```bash
python3 upload -H <host> -p <port> -s <src_path> -n <filename> -r <protocol>
```

Ejemplo:

```bash
python3 upload -H 127.0.0.1 -p 9000 -s archivo.txt -n archivo.txt -r gbn
```

---

### 3. Descargar un archivo (DOWNLOAD)

```bash
python3 download -H <host> -p <port> -d <dst_path> -n <filename> -r <protocol>
```

Ejemplo:

```bash
python3 download -H 127.0.0.1 -p 9000 -d salida.txt -n archivo.txt -r gbn
```

---

## Protocolos soportados

- `saw` → Stop & Wait  
- `gbn` → Go-Back-N  

---

## Ejecución en Mininet

### Levantar red

```bash
sudo mn --topo single,2 --link tc,loss=10
```

### Ejecutar servidor

```bash
h1 python3 start-server -H 10.0.0.1 -p 9000 -s storage
```

### Ejecutar cliente

```bash
h2 python3 upload -H 10.0.0.1 -p 9000 -s archivo.txt -n archivo.txt -r gbn
```

---

## Pruebas realizadas

Se realizaron pruebas con:

- Archivos de 100 KB, 1 MB y 5 MB  
- Pérdida de paquetes de 0% y 10%  
- Comparación entre Stop & Wait y Go-Back-N  

---

## Manejo de errores

El sistema contempla:

- Pérdida de paquetes  
- Timeouts y retransmisiones  
- Paquetes corruptos  
- Archivo inexistente  
- Mensajes de error del servidor  

---

## Concurrencia

El servidor utiliza threads para manejar múltiples transferencias simultáneamente.  
Además, se implementó deduplicación de solicitudes para evitar múltiples sesiones ante retransmisiones del cliente.

---

## Autores
### Grupo 22

- Alejo Fernández Paz - 112142
- Alén Calandria - 112103
- Santiago Arias - 112194
- Camila Miranda Vandevalle - 112777
- José Evaristo Tissera - 112788