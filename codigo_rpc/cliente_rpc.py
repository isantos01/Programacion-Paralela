import xmlrpc.client

URL_SERVIDOR = "http://127.0.0.1:8000"


def iniciar_cliente_rpc():
    """Cliente XML-RPC que solicita un numero y llama una funcion remota."""
    try:
        entrada = input("Ingrese un numero entero: ")
        numero = int(entrada)

        servidor = xmlrpc.client.ServerProxy(URL_SERVIDOR)
        resultado = servidor.calcular_cuadrado(numero)

        print(f"El cuadrado de {numero} es {resultado}")

    except ValueError:
        print("Debe ingresar un numero entero valido.")
    except ConnectionRefusedError:
        print("No fue posible conectar. Verifique que el servidor RPC este ejecutandose.")
    except xmlrpc.client.Fault as error:
        print(f"Error reportado por el servidor RPC: {error}")
    except Exception as error:
        print(f"Error inesperado en el cliente RPC: {error}")


if __name__ == "__main__":
    iniciar_cliente_rpc()
