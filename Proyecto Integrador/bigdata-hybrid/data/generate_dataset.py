"""Genera un dataset numérico de prueba. Uso: python generate_dataset.py [N] [archivo]"""
import sys, random
n = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
path = sys.argv[2] if len(sys.argv) > 2 else "input.txt"
with open(path, "w") as f:
    f.write("\n".join(str(random.randint(0, 10000)) for _ in range(n)))
print(f"Generado {path} con {n} numeros")
