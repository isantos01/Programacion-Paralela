import socket

HOST = "127.0.0.1"
PORT = 5000
BUFFER_SIZE = 1024


def iniciar_cliente():
    """Cliente TCP que envia un mensaje al servidor y espera una confirmacion."""
    mensaje = "Hola servidor, este mensaje fue enviado desde el cliente usando sockets."

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as cliente:
            cliente.connect((HOST, PORT))
            print(f"Conectado al servidor {HOST}:{PORT}")

            cliente.sendall(mensaje.encode("utf-8"))
            print(f"Mensaje enviado: {mensaje}")

            datos = cliente.recv(BUFFER_SIZE)
            respuesta = datos.decode("utf-8")
            print(f"Respuesta recibida: {respuesta}")

    except ConnectionRefusedError:
        print("No fue posible conectar. Verifique que el servidor este ejecutandose.")
    except OSError as error:
        print(f"Error de comunicacion: {error}")
    except Exception as error:
        print(f"Error inesperado en el cliente: {error}")


if __name__ == "__main__":
    iniciar_cliente()
