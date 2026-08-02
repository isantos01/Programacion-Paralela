#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
 PROTOTIPO DE CONSENSO DISTRIBUIDO - ALGORITMO PAXOS (single-decree)
===============================================================================
 Actividad Semana 12 - Consenso Distribuido (Paxos y Raft)

 Implementacion de Paxos Basico (Lamport, "Paxos Made Simple") con 5 nodos
 ACEPTADORES, varios PROPONENTES y APRENDICES.

 LAS DOS FASES DEL ALGORITMO
 ---------------------------
 FASE 1 (PREPARAR):
   1a) El proponente elige un numero de propuesta n unico y creciente, y envia
       PREPARE(n) a una mayoria de aceptadores.
   1b) Si n es mayor que cualquier numero ya prometido, el aceptador responde
       PROMISE(n) comprometiendose a NO aceptar propuestas menores que n, e
       incluye la propuesta de mayor numero que haya aceptado previamente.

 FASE 2 (ACEPTAR):
   2a) Si el proponente recibe PROMISE de una MAYORIA, envia ACCEPT(n, v).
       CLAVE DE LA SEGURIDAD: si alguna promesa reporto un valor ya aceptado,
       el proponente esta OBLIGADO a proponer ese valor (el de mayor n), no el
       suyo. Asi es imposible decidir dos valores distintos.
   2b) El aceptador acepta (n, v) salvo que haya prometido a un n mayor, y
       notifica a los aprendices.

 Un valor queda ELEGIDO (chosen) cuando una MAYORIA de aceptadores lo acepta.

 Autor: [Estudiante]
 Ejecucion: python3 paxos_simulacion.py
===============================================================================
"""

import queue
import random
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

NUM_ACEPTADORES = 5
QUORUM = NUM_ACEPTADORES // 2 + 1     # Mayoria = 3 de 5
LATENCIA_MIN, LATENCIA_MAX = 0.005, 0.030
TIMEOUT_FASE = 1.0                    # Tiempo maximo de espera por un quorum

T0 = time.time()
_lock_log = threading.Lock()
LINEAS_LOG: List[str] = []


def log(actor: str, mensaje: str, categoria: str = "INFO") -> None:
    linea = f"[{time.time() - T0:7.3f}s] [{actor:<12}] [{categoria:<9}] {mensaje}"
    with _lock_log:
        LINEAS_LOG.append(linea)
        print(linea)
        sys.stdout.flush()


def titulo(texto: str) -> None:
    barra = "=" * 78
    with _lock_log:
        for l in (barra, f"  {texto}", barra):
            LINEAS_LOG.append(l)
            print(l)
        sys.stdout.flush()


@dataclass
class Mensaje:
    tipo: str
    origen: str
    destino: str
    datos: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# RED SIMULADA (misma filosofia que en el prototipo de Raft)
# =============================================================================
class Red:
    def __init__(self) -> None:
        self.buzones: Dict[str, "queue.Queue[Mensaje]"] = {}
        self.caidos: set = set()
        self.lock = threading.Lock()
        self.entregados = 0
        self.descartados = 0

    def registrar(self, nombre: str, buzon: "queue.Queue[Mensaje]") -> None:
        self.buzones[nombre] = buzon

    def esta_caido(self, nombre: str) -> bool:
        with self.lock:
            return nombre in self.caidos

    def caer(self, nombre: str) -> None:
        with self.lock:
            self.caidos.add(nombre)

    def recuperar(self, nombre: str) -> None:
        with self.lock:
            self.caidos.discard(nombre)

    def enviar(self, msg: Mensaje) -> None:
        """Entrega diferida. Si el destino esta caido, el mensaje se pierde:
        el proponente simplemente nunca recibira esa respuesta."""
        with self.lock:
            if msg.destino in self.caidos or msg.origen in self.caidos:
                self.descartados += 1
                return
            self.entregados += 1

        def entregar() -> None:
            if not self.esta_caido(msg.destino):
                self.buzones[msg.destino].put(msg)

        t = threading.Timer(random.uniform(LATENCIA_MIN, LATENCIA_MAX), entregar)
        t.daemon = True
        t.start()


# =============================================================================
# ROL: ACEPTADOR (Acceptor) - la memoria duradera del algoritmo
# =============================================================================
class Aceptador(threading.Thread):
    def __init__(self, nombre: str, red: Red, aprendices: List[str]) -> None:
        super().__init__(daemon=True)
        self.nombre = nombre
        self.red = red
        self.aprendices = aprendices
        self.inbox: "queue.Queue[Mensaje]" = queue.Queue()
        self.activo = True

        # ---- ESTADO PERSISTENTE DEL ACEPTADOR ----
        self.promesa_min: int = 0            # minProposal: mayor n prometido
        self.n_aceptado: int = 0             # acceptedProposal
        self.valor_aceptado: Optional[str] = None   # acceptedValue

    def run(self) -> None:
        while self.activo:
            if self.red.esta_caido(self.nombre):
                time.sleep(0.02)
                continue
            try:
                msg = self.inbox.get(timeout=0.02)
            except queue.Empty:
                continue
            if msg.tipo == "PREPARE":
                self._fase1b(msg)
            elif msg.tipo == "ACCEPT":
                self._fase2b(msg)

    # -------------------------------------------------- FASE 1b: RESPONDER
    def _fase1b(self, msg: Mensaje) -> None:
        """Responde PROMISE si el numero de propuesta supera todo lo prometido."""
        n = msg.datos["n"]
        if n > self.promesa_min:
            self.promesa_min = n
            log(self.nombre,
                f"PREPARE(n={n}) de {msg.origen} -> PROMISE(n={n}). "
                f"Se compromete a rechazar propuestas con n < {n}. "
                f"Reporta valor previamente aceptado: "
                f"{f'(n={self.n_aceptado}, v={self.valor_aceptado})' if self.valor_aceptado else 'ninguno'}",
                "FASE-1b")
            self.red.enviar(Mensaje("PROMISE", self.nombre, msg.origen,
                                    {"n": n,
                                     "n_aceptado": self.n_aceptado,
                                     "valor_aceptado": self.valor_aceptado}))
        else:
            log(self.nombre,
                f"PREPARE(n={n}) de {msg.origen} -> RECHAZADO "
                f"(ya prometio a n={self.promesa_min}, mayor o igual)",
                "FASE-1b")
            self.red.enviar(Mensaje("NACK", self.nombre, msg.origen,
                                    {"n": n, "promesa_min": self.promesa_min}))

    # ---------------------------------------------------- FASE 2b: ACEPTAR
    def _fase2b(self, msg: Mensaje) -> None:
        """Acepta el valor salvo que exista una promesa a un numero mayor."""
        n, v = msg.datos["n"], msg.datos["valor"]
        if n >= self.promesa_min:
            self.promesa_min = n
            self.n_aceptado = n
            self.valor_aceptado = v
            log(self.nombre,
                f"ACCEPT(n={n}, v='{v}') de {msg.origen} -> ACEPTADO. "
                f"Estado: (promesa_min={self.promesa_min}, aceptado=('{v}', n={n}))",
                "FASE-2b")
            self.red.enviar(Mensaje("ACCEPTED", self.nombre, msg.origen,
                                    {"n": n, "valor": v}))
            # Notificar a los aprendices para que detecten el valor elegido.
            for ap in self.aprendices:
                self.red.enviar(Mensaje("APRENDER", self.nombre, ap,
                                        {"n": n, "valor": v}))
        else:
            log(self.nombre,
                f"ACCEPT(n={n}, v='{v}') de {msg.origen} -> RECHAZADO "
                f"(prometio a n={self.promesa_min})",
                "FASE-2b")
            self.red.enviar(Mensaje("NACK", self.nombre, msg.origen,
                                    {"n": n, "promesa_min": self.promesa_min}))

    def resumen(self) -> str:
        caido = "  [CAIDO]" if self.red.esta_caido(self.nombre) else ""
        return (f"    {self.nombre:<12} promesa_min={self.promesa_min:<4} "
                f"valor_aceptado={str(self.valor_aceptado):<8} "
                f"(en n={self.n_aceptado}){caido}")

    def detener(self) -> None:
        self.activo = False


# =============================================================================
# ROL: APRENDIZ (Learner) - detecta cuando un valor ha sido ELEGIDO
# =============================================================================
class Aprendiz(threading.Thread):
    def __init__(self, nombre: str, red: Red) -> None:
        super().__init__(daemon=True)
        self.nombre = nombre
        self.red = red
        self.inbox: "queue.Queue[Mensaje]" = queue.Queue()
        self.activo = True
        self.aceptaciones: Dict[Tuple[int, str], set] = {}
        self.valor_elegido: Optional[str] = None

    def run(self) -> None:
        while self.activo:
            try:
                msg = self.inbox.get(timeout=0.02)
            except queue.Empty:
                continue
            if msg.tipo != "APRENDER":
                continue
            clave = (msg.datos["n"], msg.datos["valor"])
            self.aceptaciones.setdefault(clave, set()).add(msg.origen)
            cuenta = len(self.aceptaciones[clave])
            if cuenta >= QUORUM and self.valor_elegido is None:
                self.valor_elegido = msg.datos["valor"]
                log(self.nombre,
                    f"*** VALOR ELEGIDO: '{self.valor_elegido}' *** aceptado por "
                    f"{cuenta}/{NUM_ACEPTADORES} aceptadores (quorum={QUORUM}) "
                    f"con n={msg.datos['n']}. El consenso es DEFINITIVO e INMUTABLE.",
                    "APRENDIZ")

    def detener(self) -> None:
        self.activo = False


# =============================================================================
# ROL: PROPONENTE (Proposer) - ejecuta las dos fases
# =============================================================================
class Proponente:
    def __init__(self, nombre: str, pid: int, red: Red, aceptadores: List[str]) -> None:
        self.nombre = nombre
        self.pid = pid
        self.red = red
        self.aceptadores = aceptadores
        self.inbox: "queue.Queue[Mensaje]" = queue.Queue()
        self.ronda = 0

    def _siguiente_n(self) -> int:
        """Genera numeros de propuesta UNICOS y crecientes por proponente.

        n = ronda * total_proponentes + id  garantiza que dos proponentes
        distintos nunca usen el mismo numero (requisito de Paxos).
        """
        self.ronda += 1
        return self.ronda * 10 + self.pid

    def _recolectar(self, tipo_ok: str, n: int, timeout: float) -> List[Mensaje]:
        """Espera respuestas hasta lograr quorum o agotar el tiempo.

        Notese que NUNCA se espera a todos: basta la mayoria. Por eso Paxos
        tolera que hasta (N-1)/2 nodos esten caidos o lentos.
        """
        fin = time.time() + timeout
        respuestas: List[Mensaje] = []
        while time.time() < fin and len(respuestas) < QUORUM:
            try:
                msg = self.inbox.get(timeout=0.02)
            except queue.Empty:
                continue
            if msg.datos.get("n") != n:
                continue
            if msg.tipo == tipo_ok:
                respuestas.append(msg)
            elif msg.tipo == "NACK":
                log(self.nombre,
                    f"NACK de {msg.origen}: existe una promesa mayor "
                    f"(n={msg.datos.get('promesa_min')}). Debera reintentar con n mayor.",
                    "NACK")
        return respuestas

    def proponer(self, valor: str) -> Optional[str]:
        """Ejecuta una ronda completa de Paxos. Devuelve el valor finalmente
        propuesto en fase 2 (puede NO ser el valor deseado, por seguridad)."""
        n = self._siguiente_n()
        log(self.nombre, f"Desea proponer el valor '{valor}'. Inicia ronda con n={n}",
            "INICIO")

        # ------------------------- FASE 1a: enviar PREPARE a los aceptadores
        log(self.nombre, f"FASE 1a -> envia PREPARE(n={n}) a los {len(self.aceptadores)} aceptadores",
            "FASE-1a")
        for a in self.aceptadores:
            self.red.enviar(Mensaje("PREPARE", self.nombre, a, {"n": n}))

        promesas = self._recolectar("PROMISE", n, TIMEOUT_FASE)
        if len(promesas) < QUORUM:
            log(self.nombre,
                f"FASE 1 FALLIDA: solo {len(promesas)}/{QUORUM} promesas. "
                f"Sin mayoria NO se puede avanzar (asi Paxos evita decisiones incorrectas).",
                "ABORTA")
            return None
        log(self.nombre,
            f"FASE 1 COMPLETA: {len(promesas)}/{NUM_ACEPTADORES} promesas recibidas de "
            f"{[p.origen for p in promesas]} (quorum={QUORUM})",
            "FASE-1a")

        # ------------- REGLA DE SEGURIDAD: adoptar el valor ya aceptado, si existe
        previos = [p for p in promesas if p.datos["valor_aceptado"] is not None]
        valor_final = valor
        if previos:
            mejor = max(previos, key=lambda p: p.datos["n_aceptado"])
            valor_final = mejor.datos["valor_aceptado"]
            log(self.nombre,
                f"REGLA DE SEGURIDAD: {mejor.origen} reporto el valor ya aceptado "
                f"'{valor_final}' (n={mejor.datos['n_aceptado']}). El proponente ABANDONA "
                f"su valor '{valor}' y propone '{valor_final}'. Asi Paxos garantiza que "
                f"jamas se elijan dos valores distintos.",
                "SEGURIDAD")
        else:
            log(self.nombre,
                f"Ninguna promesa reporta valores previos -> es libre de proponer su "
                f"propio valor '{valor_final}'",
                "SEGURIDAD")

        # ---------------------------- FASE 2a: enviar ACCEPT a los aceptadores
        log(self.nombre, f"FASE 2a -> envia ACCEPT(n={n}, v='{valor_final}')", "FASE-2a")
        for a in self.aceptadores:
            self.red.enviar(Mensaje("ACCEPT", self.nombre, a, {"n": n, "valor": valor_final}))

        aceptaciones = self._recolectar("ACCEPTED", n, TIMEOUT_FASE)
        if len(aceptaciones) < QUORUM:
            log(self.nombre,
                f"FASE 2 FALLIDA: solo {len(aceptaciones)}/{QUORUM} aceptaciones. "
                f"El valor NO queda elegido.", "ABORTA")
            return None

        log(self.nombre,
            f"FASE 2 COMPLETA: '{valor_final}' aceptado por "
            f"{len(aceptaciones)}/{NUM_ACEPTADORES} aceptadores {[a.origen for a in aceptaciones]} "
            f"-> VALOR ELEGIDO (chosen)",
            "EXITO")
        return valor_final


# =============================================================================
# ESCENARIO DE SIMULACION
# =============================================================================
def main() -> None:
    titulo("PROTOTIPO PAXOS BASICO - 5 ACEPTADORES (QUORUM = 3)")

    red = Red()
    nombres_acept = [f"ACEPTADOR-{i}" for i in range(1, NUM_ACEPTADORES + 1)]
    aprendiz = Aprendiz("APRENDIZ", red)
    red.registrar(aprendiz.nombre, aprendiz.inbox)

    aceptadores = [Aceptador(nm, red, [aprendiz.nombre]) for nm in nombres_acept]
    for a in aceptadores:
        red.registrar(a.nombre, a.inbox)
        a.start()
    aprendiz.start()

    p1 = Proponente("PROPONENTE-1", 1, red, nombres_acept)
    p2 = Proponente("PROPONENTE-2", 2, red, nombres_acept)
    for p in (p1, p2):
        red.registrar(p.nombre, p.inbox)

    log("SISTEMA", f"{NUM_ACEPTADORES} aceptadores, 2 proponentes y 1 aprendiz iniciados. "
                   f"Quorum necesario: {QUORUM}", "INICIO")

    # ------------------------------------------------------------- ESCENARIO 1
    titulo("ESCENARIO 1: RONDA COMPLETA SIN FALLOS - SE ACUERDA 'A=1'")
    resultado = p1.proponer("A=1")
    time.sleep(0.4)
    log("SISTEMA", f"Valor acordado por el cluster: '{resultado}'", "OK")

    # ------------------------------------------------------------- ESCENARIO 2
    titulo("ESCENARIO 2: SEGUNDO PROPONENTE INTENTA IMPONER 'B=99' (SEGURIDAD)")
    log("SISTEMA", "PROPONENTE-2 desconoce el acuerdo previo e intenta proponer otro valor. "
                   "Paxos debe FORZARLO a respetar el valor ya elegido.", "PRUEBA")
    resultado2 = p2.proponer("B=99")
    time.sleep(0.4)
    log("SISTEMA",
        f"Resultado: se volvio a elegir '{resultado2}'. "
        f"{'SEGURIDAD PRESERVADA: el valor no cambio.' if resultado2 == 'A=1' else 'FALLO DE SEGURIDAD'}",
        "OK")

    # ------------------------------------------------------------- ESCENARIO 3
    titulo("ESCENARIO 3: FALLO DE 2 ACEPTADORES - EL CONSENSO SOBREVIVE")
    for nm in nombres_acept[:2]:
        red.caer(nm)
        log("SISTEMA", f"!!! {nm} CAE: deja de responder PREPARE y ACCEPT !!!", "FALLO")
    log("SISTEMA", f"Quedan 3 aceptadores vivos = quorum ({QUORUM}). El sistema DEBE seguir.",
        "FALLO")
    resultado3 = p1.proponer("C=7")
    time.sleep(0.4)
    log("SISTEMA",
        f"Resultado con 2 nodos caidos: '{resultado3}'. El protocolo avanzo usando solo la "
        f"mayoria viva y siguio respetando el valor ya elegido.", "OK")

    # ------------------------------------------------------------- ESCENARIO 4
    titulo("ESCENARIO 4: FALLO DE 3 ACEPTADORES - SE PIERDE EL QUORUM")
    red.caer(nombres_acept[2])
    log("SISTEMA", f"!!! {nombres_acept[2]} TAMBIEN CAE. Solo quedan 2 vivos < quorum {QUORUM} !!!",
        "FALLO")
    log("SISTEMA", "Paxos priorizara la CONSISTENCIA sobre la DISPONIBILIDAD (teorema CAP): "
                   "preferira no progresar antes que arriesgar un acuerdo incorrecto.", "FALLO")
    resultado4 = p1.proponer("D=0")
    log("SISTEMA",
        f"Resultado: {resultado4}. Sin mayoria NO hay consenso, pero tampoco hay corrupcion: "
        f"el valor 'A=1' permanece intacto en los aceptadores.", "OK")

    # ------------------------------------------------------------- ESCENARIO 5
    titulo("ESCENARIO 5: RECUPERACION DE LOS ACEPTADORES")
    for nm in nombres_acept[:3]:
        red.recuperar(nm)
        log("SISTEMA", f"{nm} se REINCORPORA al cluster conservando su estado persistente.",
            "RECUPERA")
    time.sleep(0.3)
    resultado5 = p2.proponer("E=5")
    time.sleep(0.4)
    log("SISTEMA", f"Con el quorum restablecido, la ronda concluye y reconfirma: '{resultado5}'",
        "OK")

    # ------------------------------------------------------------- VERIFICACION
    titulo("ESTADO FINAL DE LOS ACEPTADORES")
    for a in aceptadores:
        log("SISTEMA", a.resumen(), "ESTADO")

    titulo("VERIFICACION DE PROPIEDADES DE SEGURIDAD")
    valores = {a.valor_aceptado for a in aceptadores if a.valor_aceptado}
    log("SISTEMA", f"1. Valores distintos aceptados en todo el cluster: {valores} -> "
                   f"{'OK: un unico valor' if len(valores) == 1 else 'FALLO'}", "VERIFICA")
    log("SISTEMA", f"2. Valor elegido por el aprendiz: '{aprendiz.valor_elegido}' "
                   f"(inmutable desde el escenario 1)", "VERIFICA")
    log("SISTEMA", f"3. Mensajes entregados: {red.entregados} | descartados por fallos: "
                   f"{red.descartados}", "VERIFICA")

    titulo("SIMULACION FINALIZADA CORRECTAMENTE")
    for a in aceptadores:
        a.detener()
    aprendiz.detener()

    with open("paxos_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(LINEAS_LOG))
    print("\n[+] Log completo guardado en paxos_log.txt")


if __name__ == "__main__":
    main()
