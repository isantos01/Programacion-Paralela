import socket

HOST = "127.0.0.1"
PORT = 5000
BUFFER_SIZE = 1024


def iniciar_servidor():
    """Inicia un servidor TCP que recibe un mensaje y responde una confirmacion."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor.bind((HOST, PORT))
            servidor.listen(1)
            print(f"Servidor socket escuchando en {HOST}:{PORT}...")

            conexion, direccion_cliente = servidor.accept()
            with conexion:
                print(f"Conexion recibida desde {direccion_cliente}")
                datos = conexion.recv(BUFFER_SIZE)

                if not datos:
                    print("No se recibieron datos del cliente.")
                    return

                mensaje = datos.decode("utf-8")
                print(f"Mensaje recibido del cliente: {mensaje}")

                respuesta = "Confirmacion del servidor: mensaje recibido correctamente."
                conexion.sendall(respuesta.encode("utf-8"))
                print("Respuesta enviada al cliente.")

    except OSError as error:
        print(f"Error de socket o puerto: {error}")
    except Exception as error:
        print(f"Error inesperado en el servidor: {error}")


if __name__ == "__main__":
    iniciar_servidor()
