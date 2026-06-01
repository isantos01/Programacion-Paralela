from xmlrpc.server import SimpleXMLRPCServer

HOST = "127.0.0.1"
PORT = 8000


def calcular_cuadrado(numero):
    """Funcion remota que recibe un entero y devuelve su cuadrado."""
    numero = int(numero)
    return numero * numero


def iniciar_servidor_rpc():
    """Inicia un servidor XML-RPC y publica la funcion calcular_cuadrado."""
    try:
        with SimpleXMLRPCServer((HOST, PORT), allow_none=True, logRequests=True) as servidor:
            servidor.register_function(calcular_cuadrado, "calcular_cuadrado")
            print(f"Servidor RPC escuchando en http://{HOST}:{PORT}")
            print("Funcion remota disponible: calcular_cuadrado(numero)")
            servidor.serve_forever()
    except OSError as error:
        print(f"Error al iniciar el servidor RPC: {error}")
    except KeyboardInterrupt:
        print("\nServidor RPC detenido por el usuario.")


if __name__ == "__main__":
    iniciar_servidor_rpc()
