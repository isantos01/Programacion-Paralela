#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
 PROTOTIPO DE CONSENSO DISTRIBUIDO - ALGORITMO RAFT
===============================================================================
 Actividad Semana 12 - Consenso Distribuido (Paxos y Raft)

 Este programa simula un cluster de 5 nodos que ejecutan el algoritmo Raft
 (Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm").

 Se implementan los tres subproblemas del algoritmo:
   1) ELECCION DE LIDER  -> RPC RequestVote  (solicitud de voto)
   2) REPLICACION DE LOG -> RPC AppendEntries (anexar entradas / heartbeat)
   3) SEGURIDAD          -> reglas de terms, voto unico por term, chequeo de
                            "log al menos tan actualizado" y commit por mayoria.

 MODELO DE EJECUCION
 -------------------
 Cada nodo es un ACTOR: un hilo independiente con su propia cola de mensajes
 (inbox). No existe memoria compartida entre nodos; la unica forma de
 comunicarse es enviando mensajes a traves de la clase `Red`, que simula
 latencia de red y la caida (crash) de nodos descartando sus mensajes.
 Esto evita interbloqueos y modela de forma realista un sistema distribuido.

 Autor: [Estudiante]
 Ejecucion: python3 raft_simulacion.py
===============================================================================
"""

import queue
import random
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# =============================================================================
# PARAMETROS DE CONFIGURACION DEL CLUSTER
# =============================================================================
NUM_NODOS = 5                      # Cluster de 5 nodos -> mayoria (quorum) = 3
QUORUM = NUM_NODOS // 2 + 1        # Mayoria estricta: floor(N/2) + 1

# Los tiempos estan "inflados" respecto a un sistema real (que usa milisegundos)
# para que los logs sean legibles por un humano durante la demostracion.
HEARTBEAT_INTERVAL = 0.25          # Cada cuanto el lider envia latidos
ELECCION_TIMEOUT_MIN = 0.80        # Timeout de eleccion minimo
ELECCION_TIMEOUT_MAX = 1.60        # Timeout de eleccion maximo (aleatorio)
LATENCIA_RED_MIN = 0.005           # Latencia minima simulada de la red
LATENCIA_RED_MAX = 0.030           # Latencia maxima simulada de la red

# Estados posibles de un nodo Raft (maquina de estados finita)
SEGUIDOR = "SEGUIDOR"              # Follower
CANDIDATO = "CANDIDATO"            # Candidate
LIDER = "LIDER"                    # Leader

T0 = time.time()                   # Marca de tiempo inicial de la simulacion
_lock_log = threading.Lock()       # Protege la escritura concurrente del log
LINEAS_LOG: List[str] = []         # Buffer con todas las lineas registradas


def log(nodo: str, mensaje: str, categoria: str = "INFO") -> None:
    """Registra un evento con marca de tiempo relativa al inicio de la simulacion.

    Todos los hilos escriben aqui, por eso se protege con un lock: sin el, las
    lineas de distintos nodos se entremezclarian a mitad de escritura.
    """
    linea = f"[{time.time() - T0:7.3f}s] [{nodo:<9}] [{categoria:<9}] {mensaje}"
    with _lock_log:
        LINEAS_LOG.append(linea)
        print(linea)
        sys.stdout.flush()


def titulo(texto: str) -> None:
    """Imprime un separador visual para delimitar las fases de la simulacion."""
    barra = "=" * 78
    with _lock_log:
        for linea in (barra, f"  {texto}", barra):
            LINEAS_LOG.append(linea)
            print(linea)
        sys.stdout.flush()


# =============================================================================
# ESTRUCTURAS DE DATOS
# =============================================================================
@dataclass
class EntradaLog:
    """Una entrada del log replicado de Raft.

    term:    term en el que la entrada fue creada por un lider. Es la clave de
             la seguridad de Raft: permite detectar logs divergentes.
    comando: la operacion a aplicar a la maquina de estados, ej. "A=1".
    """
    term: int
    comando: str

    def __repr__(self) -> str:
        return f"(t{self.term}:{self.comando})"


@dataclass
class Mensaje:
    """Mensaje (RPC) intercambiado entre nodos a traves de la red simulada."""
    tipo: str                                  # Tipo de RPC
    origen: int                                # ID del nodo emisor
    destino: int                               # ID del nodo receptor
    datos: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# RED SIMULADA
# =============================================================================
class Red:
    """Canal de comunicacion entre nodos con latencia y fallos simulados.

    Modela una red asincrona no confiable pero SIN corrupcion de mensajes
    (modelo fail-stop / crash), que es exactamente el modelo de fallos que
    Raft y Paxos toleran.
    """

    def __init__(self) -> None:
        self.nodos: Dict[int, "NodoRaft"] = {}
        self.caidos: set = set()          # Nodos "apagados" (no envian ni reciben)
        self.lock = threading.Lock()
        self.entregados = 0
        self.descartados = 0

    def registrar(self, nodo: "NodoRaft") -> None:
        self.nodos[nodo.id] = nodo

    def esta_caido(self, nid: int) -> bool:
        with self.lock:
            return nid in self.caidos

    def caer(self, nid: int) -> None:
        """Simula el crash de un nodo: deja de enviar y recibir mensajes."""
        with self.lock:
            self.caidos.add(nid)

    def recuperar(self, nid: int) -> None:
        """Simula el reinicio de un nodo previamente caido."""
        with self.lock:
            self.caidos.discard(nid)

    def enviar(self, msg: Mensaje) -> None:
        """Entrega un mensaje tras una latencia aleatoria.

        Si el emisor o el receptor estan caidos, el mensaje se pierde
        silenciosamente: asi es como Raft "percibe" un fallo (por ausencia de
        respuesta, nunca por una notificacion explicita).
        """
        with self.lock:
            if msg.origen in self.caidos or msg.destino in self.caidos:
                self.descartados += 1
                return
            self.entregados += 1

        def entregar() -> None:
            # Se vuelve a verificar al momento de entregar: el nodo pudo caer
            # mientras el mensaje viajaba por la red.
            if not self.esta_caido(msg.destino) and not self.esta_caido(msg.origen):
                self.nodos[msg.destino].inbox.put(msg)

        retardo = random.uniform(LATENCIA_RED_MIN, LATENCIA_RED_MAX)
        t = threading.Timer(retardo, entregar)
        t.daemon = True
        t.start()


# =============================================================================
# NODO RAFT
# =============================================================================
class NodoRaft(threading.Thread):
    """Implementacion de un servidor Raft como hilo independiente."""

    def __init__(self, nid: int, pares: List[int], red: Red) -> None:
        super().__init__(daemon=True)
        self.id = nid
        self.nombre = f"NODO-{nid}"
        self.pares = [p for p in pares if p != nid]
        self.red = red
        self.inbox: "queue.Queue[Mensaje]" = queue.Queue()
        self.activo = True

        # ---- ESTADO PERSISTENTE (sobrevive a un reinicio del nodo) ----
        self.term_actual = 0                      # currentTerm
        self.voto_por: Optional[int] = None       # votedFor
        self.log: List[EntradaLog] = []           # log[] (indice 1..N logico)

        # ---- ESTADO VOLATIL EN TODOS LOS NODOS ----
        self.estado = SEGUIDOR
        self.commit_index = 0                     # ultima entrada comprometida
        self.last_applied = 0                     # ultima aplicada a la maquina
        self.maquina_estados: Dict[str, str] = {} # Maquina de estados replicada
        self.lider_conocido: Optional[int] = None

        # ---- ESTADO VOLATIL SOLO EN EL LIDER ----
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        # ---- TEMPORIZADORES ----
        self.votos_recibidos: set = set()
        self._reiniciar_timeout_eleccion()
        self.proximo_heartbeat = 0.0

    # ------------------------------------------------------------------ utils
    def _reiniciar_timeout_eleccion(self) -> None:
        """Fija un nuevo timeout ALEATORIO de eleccion.

        La aleatoriedad es esencial: evita que todos los seguidores se
        conviertan en candidatos al mismo tiempo y se dividan el voto
        indefinidamente (split vote).
        """
        self.deadline_eleccion = time.time() + random.uniform(
            ELECCION_TIMEOUT_MIN, ELECCION_TIMEOUT_MAX
        )

    @property
    def ultimo_indice(self) -> int:
        """Indice de la ultima entrada del log (0 si el log esta vacio)."""
        return len(self.log)

    @property
    def ultimo_term(self) -> int:
        """Term de la ultima entrada del log (0 si el log esta vacio)."""
        return self.log[-1].term if self.log else 0

    def _enviar(self, destino: int, tipo: str, **datos: Any) -> None:
        self.red.enviar(Mensaje(tipo=tipo, origen=self.id, destino=destino, datos=datos))

    def _pasar_a_seguidor(self, term: int, motivo: str = "") -> None:
        """Regla universal de Raft: si se ve un term mayor, se retrocede a seguidor."""
        cambio = self.estado != SEGUIDOR
        if term > self.term_actual:
            self.term_actual = term
            self.voto_por = None
        self.estado = SEGUIDOR
        if cambio:
            log(self.nombre, f"Pasa a SEGUIDOR en term {self.term_actual}. {motivo}", "ESTADO")

    # ------------------------------------------------------------- ciclo vida
    def run(self) -> None:
        """Bucle principal del actor: procesa mensajes y vence temporizadores."""
        while self.activo:
            # Un nodo "caido" no procesa nada: simula estar apagado.
            if self.red.esta_caido(self.id):
                time.sleep(0.02)
                self._reiniciar_timeout_eleccion()   # al revivir no dispara de inmediato
                continue

            try:
                msg = self.inbox.get(timeout=0.01)
                self._procesar(msg)
            except queue.Empty:
                pass

            ahora = time.time()
            if self.estado == LIDER:
                if ahora >= self.proximo_heartbeat:
                    self._enviar_append_entries()
                    self.proximo_heartbeat = ahora + HEARTBEAT_INTERVAL
            elif ahora >= self.deadline_eleccion:
                # No hemos sabido del lider a tiempo -> iniciar eleccion.
                self._iniciar_eleccion()

            self._aplicar_comprometidas()

    def detener(self) -> None:
        self.activo = False

    # ------------------------------------------------- FASE 1: ELECCION LIDER
    def _iniciar_eleccion(self) -> None:
        """Convierte el nodo en CANDIDATO e inicia una nueva eleccion.

        Pasos (paper de Raft, seccion 5.2):
          1. Incrementar currentTerm
          2. Votar por si mismo
          3. Reiniciar el temporizador de eleccion
          4. Enviar RequestVote a todos los demas nodos
        """
        self.estado = CANDIDATO
        self.term_actual += 1
        self.voto_por = self.id
        self.votos_recibidos = {self.id}
        self._reiniciar_timeout_eleccion()

        log(self.nombre,
            f"Timeout de eleccion vencido -> se postula como CANDIDATO en term {self.term_actual} "
            f"(vota por si mismo, 1/{QUORUM} votos)",
            "ELECCION")

        for p in self.pares:
            self._enviar(p, "RequestVote",
                         term=self.term_actual,
                         candidato=self.id,
                         ultimo_indice=self.ultimo_indice,
                         ultimo_term=self.ultimo_term)

    def _manejar_request_vote(self, msg: Mensaje) -> None:
        """RPC RequestVote (receptor): decide si concede el voto al candidato."""
        d = msg.datos
        if d["term"] > self.term_actual:
            self._pasar_a_seguidor(d["term"], f"Term superior visto de NODO-{msg.origen}.")

        conceder = False
        if d["term"] < self.term_actual:
            motivo = f"term {d['term']} obsoleto (actual {self.term_actual})"
        elif self.voto_por is not None and self.voto_por != d["candidato"]:
            motivo = f"ya voto por NODO-{self.voto_por} en term {self.term_actual}"
        else:
            # RESTRICCION DE ELECCION (seccion 5.4.1): solo se vota por un
            # candidato cuyo log este AL MENOS TAN ACTUALIZADO como el propio.
            # Esto garantiza que el nuevo lider contenga todas las entradas
            # ya comprometidas.
            mas_actual = (d["ultimo_term"] > self.ultimo_term or
                          (d["ultimo_term"] == self.ultimo_term and
                           d["ultimo_indice"] >= self.ultimo_indice))
            if mas_actual:
                conceder = True
                self.voto_por = d["candidato"]
                self._reiniciar_timeout_eleccion()
                motivo = "log del candidato al menos tan actualizado"
            else:
                motivo = "log del candidato desactualizado (restriccion 5.4.1)"

        log(self.nombre,
            f"RequestVote de NODO-{msg.origen} (term {d['term']}) -> "
            f"{'VOTO CONCEDIDO' if conceder else 'VOTO DENEGADO'}: {motivo}",
            "VOTO")

        self._enviar(msg.origen, "RequestVoteResp",
                     term=self.term_actual, concedido=conceder)

    def _manejar_respuesta_voto(self, msg: Mensaje) -> None:
        """RPC RequestVote (emisor): cuenta los votos recibidos."""
        d = msg.datos
        if d["term"] > self.term_actual:
            self._pasar_a_seguidor(d["term"], "Respuesta con term superior.")
            return
        # Respuesta tardia de una eleccion anterior: se ignora.
        if self.estado != CANDIDATO or d["term"] != self.term_actual:
            return

        if d["concedido"]:
            self.votos_recibidos.add(msg.origen)
            log(self.nombre,
                f"Recibe voto de NODO-{msg.origen} "
                f"({len(self.votos_recibidos)}/{QUORUM} votos necesarios)",
                "ELECCION")
            if len(self.votos_recibidos) >= QUORUM:
                self._convertirse_en_lider()

    def _convertirse_en_lider(self) -> None:
        """El candidato alcanzo la mayoria: asume el liderazgo del term."""
        self.estado = LIDER
        self.lider_conocido = self.id
        # Optimistamente se asume que los seguidores tienen el mismo log;
        # si no, AppendEntries lo corregira retrocediendo next_index.
        self.next_index = {p: self.ultimo_indice + 1 for p in self.pares}
        self.match_index = {p: 0 for p in self.pares}
        self.proximo_heartbeat = 0.0

        log(self.nombre,
            f"*** ELECTO LIDER del term {self.term_actual} con "
            f"{len(self.votos_recibidos)}/{NUM_NODOS} votos "
            f"(quorum={QUORUM}) *** votantes={sorted(self.votos_recibidos)}",
            "LIDERAZGO")

    # -------------------------------------------- FASE 2: REPLICACION DEL LOG
    def _enviar_append_entries(self) -> None:
        """El lider envia AppendEntries a cada seguidor (latido o con entradas)."""
        for p in self.pares:
            prev_indice = self.next_index.get(p, 1) - 1
            prev_term = self.log[prev_indice - 1].term if prev_indice > 0 else 0
            entradas = self.log[prev_indice:]     # entradas que le faltan al seguidor

            if entradas:
                log(self.nombre,
                    f"Replicando {len(entradas)} entrada(s) {entradas} hacia NODO-{p} "
                    f"(prevIndice={prev_indice}, prevTerm={prev_term})",
                    "REPLICA")

            self._enviar(p, "AppendEntries",
                         term=self.term_actual,
                         lider=self.id,
                         prev_indice=prev_indice,
                         prev_term=prev_term,
                         entradas=[EntradaLog(e.term, e.comando) for e in entradas],
                         commit_lider=self.commit_index)

    def _manejar_append_entries(self, msg: Mensaje) -> None:
        """RPC AppendEntries (receptor): valida consistencia y anexa entradas."""
        d = msg.datos

        # 1) Term obsoleto -> se rechaza y se informa el term real.
        if d["term"] < self.term_actual:
            self._enviar(msg.origen, "AppendEntriesResp",
                         term=self.term_actual, exito=False, match=0)
            return

        # 2) Term valido: reconocemos al emisor como lider legitimo.
        if d["term"] > self.term_actual or self.estado != SEGUIDOR:
            self._pasar_a_seguidor(d["term"], f"AppendEntries de lider NODO-{d['lider']}.")
        self.term_actual = d["term"]
        self._reiniciar_timeout_eleccion()        # el lider esta vivo: no hay eleccion

        if self.lider_conocido != d["lider"]:
            self.lider_conocido = d["lider"]
            log(self.nombre, f"Reconoce a NODO-{d['lider']} como LIDER del term {d['term']}",
                "ESTADO")

        # 3) CHEQUEO DE CONSISTENCIA DEL LOG (Log Matching Property):
        #    la entrada previa debe coincidir en indice Y term.
        if d["prev_indice"] > 0:
            if (len(self.log) < d["prev_indice"] or
                    self.log[d["prev_indice"] - 1].term != d["prev_term"]):
                log(self.nombre,
                    f"Rechaza AppendEntries: inconsistencia en indice {d['prev_indice']} "
                    f"(mi log tiene {len(self.log)} entradas). El lider retrocedera.",
                    "REPLICA")
                self._enviar(msg.origen, "AppendEntriesResp",
                             term=self.term_actual, exito=False, match=0)
                return

        # 4) Anexar entradas nuevas, eliminando las que entren en conflicto.
        if d["entradas"]:
            self.log = self.log[:d["prev_indice"]] + list(d["entradas"])
            log(self.nombre,
                f"Anexa {len(d['entradas'])} entrada(s) {d['entradas']}. "
                f"Log = {self.log}",
                "REPLICA")

        # 5) Avanzar el commit index segun lo indicado por el lider.
        if d["commit_lider"] > self.commit_index:
            self.commit_index = min(d["commit_lider"], self.ultimo_indice)

        self._enviar(msg.origen, "AppendEntriesResp",
                     term=self.term_actual, exito=True, match=self.ultimo_indice)

    def _manejar_respuesta_append(self, msg: Mensaje) -> None:
        """RPC AppendEntries (emisor): actualiza indices y evalua el commit."""
        d = msg.datos
        if d["term"] > self.term_actual:
            self._pasar_a_seguidor(d["term"], "Seguidor con term superior.")
            return
        if self.estado != LIDER:
            return

        if d["exito"]:
            self.match_index[msg.origen] = d["match"]
            self.next_index[msg.origen] = d["match"] + 1
            self._evaluar_commit()
        else:
            # Retroceder para encontrar el punto de coincidencia del log.
            self.next_index[msg.origen] = max(1, self.next_index.get(msg.origen, 1) - 1)

    def _evaluar_commit(self) -> None:
        """Comprueba si alguna entrada alcanzo replicacion en la MAYORIA.

        Regla de seguridad (seccion 5.4.2): el lider solo compromete entradas
        de SU PROPIO term por conteo de replicas; las anteriores se comprometen
        indirectamente.
        """
        for n in range(self.ultimo_indice, self.commit_index, -1):
            replicas = 1 + sum(1 for p in self.pares if self.match_index.get(p, 0) >= n)
            if replicas >= QUORUM and self.log[n - 1].term == self.term_actual:
                self.commit_index = n
                log(self.nombre,
                    f"*** CONSENSO ALCANZADO *** Entrada #{n} '{self.log[n-1].comando}' "
                    f"replicada en {replicas}/{NUM_NODOS} nodos (quorum={QUORUM}) -> COMPROMETIDA",
                    "COMMIT")
                break

    def _aplicar_comprometidas(self) -> None:
        """Aplica a la maquina de estados las entradas ya comprometidas."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            comando = self.log[self.last_applied - 1].comando
            if "=" in comando:
                clave, valor = comando.split("=", 1)
                self.maquina_estados[clave.strip()] = valor.strip()
            log(self.nombre,
                f"Aplica entrada #{self.last_applied} '{comando}' -> "
                f"estado = {self.maquina_estados}",
                "APLICAR")

    # ------------------------------------------------------ interfaz cliente
    def solicitud_cliente(self, comando: str) -> bool:
        """Recibe una peticion del cliente. Solo el LIDER puede aceptarla."""
        if self.estado != LIDER:
            log(self.nombre,
                f"Rechaza peticion '{comando}': no soy el lider "
                f"(lider conocido: NODO-{self.lider_conocido})",
                "CLIENTE")
            return False
        self.log.append(EntradaLog(self.term_actual, comando))
        log(self.nombre,
            f"CLIENTE propone '{comando}'. Se anexa como entrada #{self.ultimo_indice} "
            f"en term {self.term_actual}. Iniciando replicacion...",
            "CLIENTE")
        self.proximo_heartbeat = 0.0     # replicar inmediatamente
        return True

    def _procesar(self, msg: Mensaje) -> None:
        """Despachador de mensajes entrantes."""
        if msg.tipo == "RequestVote":
            self._manejar_request_vote(msg)
        elif msg.tipo == "RequestVoteResp":
            self._manejar_respuesta_voto(msg)
        elif msg.tipo == "AppendEntries":
            self._manejar_append_entries(msg)
        elif msg.tipo == "AppendEntriesResp":
            self._manejar_respuesta_append(msg)

    def resumen(self) -> str:
        marca = ">>>" if self.estado == LIDER else "   "
        caido = "  [CAIDO]" if self.red.esta_caido(self.id) else ""
        return (f"{marca} {self.nombre:<8} estado={self.estado:<9} term={self.term_actual} "
                f"commit={self.commit_index} log={self.log} "
                f"maquina_estados={self.maquina_estados}{caido}")


# =============================================================================
# UTILIDADES DE LA SIMULACION
# =============================================================================
def esperar_lider(nodos: List[NodoRaft], timeout: float = 10.0) -> Optional[NodoRaft]:
    """Espera hasta que exista un lider estable y lo devuelve."""
    fin = time.time() + timeout
    while time.time() < fin:
        lideres = [n for n in nodos
                   if n.estado == LIDER and not n.red.esta_caido(n.id)]
        if len(lideres) == 1:
            time.sleep(0.4)                    # dejar que se estabilice
            if lideres[0].estado == LIDER:
                return lideres[0]
        time.sleep(0.05)
    return None


def imprimir_estado(nodos: List[NodoRaft], titulo_txt: str) -> None:
    titulo(titulo_txt)
    for n in nodos:
        log("SISTEMA", n.resumen(), "ESTADO")


def verificar_consistencia(nodos: List[NodoRaft]) -> bool:
    """Comprueba la propiedad de SEGURIDAD: ningun nodo vivo aplica valores
    distintos en la misma posicion del log."""
    vivos = [n for n in nodos if not n.red.esta_caido(n.id)]
    referencia = max(vivos, key=lambda n: n.commit_index)
    ok = True
    for n in vivos:
        prefijo = [e.comando for e in n.log[:min(n.commit_index, referencia.commit_index)]]
        esperado = [e.comando for e in referencia.log[:min(n.commit_index, referencia.commit_index)]]
        if prefijo != esperado:
            ok = False
            log("SISTEMA", f"INCONSISTENCIA en {n.nombre}: {prefijo} != {esperado}", "ERROR")
    return ok


# =============================================================================
# ESCENARIO DE SIMULACION
# =============================================================================
def main() -> None:
    random.seed()  # usar time.time() como semilla; cambie a un entero para repetir

    titulo("PROTOTIPO RAFT - CLUSTER DE 5 NODOS (QUORUM = 3)")
    log("SISTEMA", f"Iniciando {NUM_NODOS} nodos. Mayoria necesaria: {QUORUM}", "INICIO")
    log("SISTEMA", "Todos los nodos arrancan como SEGUIDOR en term 0 con el log vacio", "INICIO")

    red = Red()
    ids = list(range(1, NUM_NODOS + 1))
    nodos = [NodoRaft(i, ids, red) for i in ids]
    for n in nodos:
        red.registrar(n)
    for n in nodos:
        n.start()

    # ------------------------------------------------------------- ESCENARIO 1
    titulo("ESCENARIO 1: ELECCION DEL LIDER INICIAL")
    log("SISTEMA", "Sin lider, los seguidores agotan su timeout aleatorio y se postulan...",
        "INICIO")
    lider = esperar_lider(nodos)
    if lider is None:
        log("SISTEMA", "No se logro elegir lider", "ERROR")
        return
    log("SISTEMA", f"RESULTADO: {lider.nombre} es el lider del term {lider.term_actual}", "OK")

    # ------------------------------------------------------------- ESCENARIO 2
    titulo("ESCENARIO 2: PROPUESTA Y CONSENSO SOBRE EL VALOR 'A=1'")
    log("CLIENTE", f"Envia la operacion 'A=1' al lider {lider.nombre}", "CLIENTE")
    lider.solicitud_cliente("A=1")
    time.sleep(1.5)
    imprimir_estado(nodos, "ESTADO TRAS EL CONSENSO DE 'A=1'")

    # ------------------------------------------------------------- ESCENARIO 3
    titulo("ESCENARIO 3: UN SEGUIDOR RECHAZA PETICIONES DEL CLIENTE")
    seguidor = next(n for n in nodos if n.estado == SEGUIDOR)
    log("CLIENTE", f"Envia por error la operacion 'X=9' al seguidor {seguidor.nombre}", "CLIENTE")
    seguidor.solicitud_cliente("X=9")
    log("SISTEMA", "Solo el lider acepta escrituras: se preserva un unico punto de orden",
        "OK")
    time.sleep(0.5)

    # ------------------------------------------------------------- ESCENARIO 4
    titulo("ESCENARIO 4: FALLO DEL LIDER (CRASH) Y NUEVA ELECCION")
    lider_caido = lider
    term_anterior = lider.term_actual
    log("SISTEMA", f"!!! SIMULANDO CAIDA DEL LIDER {lider_caido.nombre} !!! "
                   f"Deja de enviar heartbeats y de responder.", "FALLO")
    red.caer(lider_caido.id)
    log("SISTEMA", "Los seguidores dejaran de recibir latidos y agotaran su timeout...",
        "FALLO")

    nuevo_lider = esperar_lider([n for n in nodos if n.id != lider_caido.id])
    if nuevo_lider is None:
        log("SISTEMA", "No se logro elegir un nuevo lider", "ERROR")
        return
    log("SISTEMA",
        f"RESULTADO: el cluster se recupero solo. {nuevo_lider.nombre} es el nuevo LIDER "
        f"(term {nuevo_lider.term_actual} > term anterior {term_anterior}). "
        f"Quedan {NUM_NODOS - 1} nodos vivos >= quorum {QUORUM}: el servicio CONTINUA.",
        "OK")

    # ------------------------------------------------------------- ESCENARIO 5
    titulo("ESCENARIO 5: EL CONSENSO CONTINUA SIN EL NODO CAIDO ('B=2')")
    log("CLIENTE", f"Envia la operacion 'B=2' al nuevo lider {nuevo_lider.nombre}", "CLIENTE")
    nuevo_lider.solicitud_cliente("B=2")
    time.sleep(1.5)
    imprimir_estado(nodos, "ESTADO CON EL LIDER ORIGINAL AUN CAIDO")
    log("SISTEMA",
        f"NOTA: {lider_caido.nombre} todavia se cree LIDER del term {term_anterior} "
        f"(lider obsoleto o 'stale leader'), pero esto es INOFENSIVO: aislado no alcanza "
        f"quorum, no puede comprometer nada, y al reconectarse vera el term superior y "
        f"retrocedera automaticamente a SEGUIDOR.", "SEGURIDAD")

    # ------------------------------------------------------------- ESCENARIO 6
    titulo("ESCENARIO 6: RECUPERACION DEL NODO CAIDO Y PUESTA AL DIA")
    log("SISTEMA", f"Reiniciando {lider_caido.nombre}. Vuelve como SEGUIDOR con el log "
                   f"desactualizado: {lider_caido.log}", "RECUPERA")
    lider_caido._pasar_a_seguidor(lider_caido.term_actual, "Reinicio del nodo.")
    lider_caido.estado = SEGUIDOR
    red.recuperar(lider_caido.id)
    log("SISTEMA", "El lider le enviara AppendEntries con las entradas faltantes...",
        "RECUPERA")
    time.sleep(2.0)
    imprimir_estado(nodos, "ESTADO FINAL: TODOS LOS NODOS CONVERGEN AL MISMO LOG")

    # ------------------------------------------------------------- VERIFICACION
    titulo("VERIFICACION DE PROPIEDADES DE SEGURIDAD")
    consistente = verificar_consistencia(nodos)
    estados = [str(sorted(n.maquina_estados.items())) for n in nodos]
    todos_iguales = len(set(estados)) == 1

    log("SISTEMA", f"1. Consistencia de logs comprometidos: {'OK' if consistente else 'FALLO'}",
        "VERIFICA")
    log("SISTEMA", f"2. Maquinas de estado identicas en los {NUM_NODOS} nodos: "
                   f"{'OK' if todos_iguales else 'FALLO'} -> {sorted(nodos[0].maquina_estados.items())}",
        "VERIFICA")
    lideres_por_term = len([n for n in nodos if n.estado == LIDER])
    log("SISTEMA", f"3. Un unico lider vigente: {'OK' if lideres_por_term == 1 else 'FALLO'} "
                   f"({lideres_por_term} lider)", "VERIFICA")
    log("SISTEMA", f"4. Mensajes entregados por la red: {red.entregados} | "
                   f"descartados por nodos caidos: {red.descartados}", "VERIFICA")

    titulo("SIMULACION FINALIZADA CORRECTAMENTE")
    for n in nodos:
        n.detener()

    with open("raft_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(LINEAS_LOG))
    print("\n[+] Log completo guardado en raft_log.txt")


if __name__ == "__main__":
    main()
