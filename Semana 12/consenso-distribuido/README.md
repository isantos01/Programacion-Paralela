# Consenso Distribuido: Paxos y Raft

Prototipos funcionales de los dos algoritmos clásicos de consenso distribuido, con simulación de fallos y verificación automática de las propiedades de seguridad.

**Actividad Semana 12** · Sistemas Distribuidos · Modalidad individual

---

## Contenido del repositorio

```
consenso-distribuido/
├── README.md                     Este archivo
├── raft/
│   └── raft_simulacion.py        Prototipo Raft: 5 nodos, elección de líder + replicación de log
├── paxos/
│   └── paxos_simulacion.py       Prototipo Paxos: fases prepare/accept con 5 aceptadores

```

---

## Requisitos

- **Python 3.8 o superior**
- **Sin dependencias externas**: ambos prototipos usan únicamente la biblioteca estándar (`threading`, `queue`, `random`, `dataclasses`).

Verificar la versión instalada:

```bash
python3 --version
```

---

## Ejecución

```bash
git clone https://github.com/USUARIO/consenso-distribuido.git
cd consenso-distribuido
```

### Prototipo de Raft (~10 segundos)

```bash
cd raft
python3 raft_simulacion.py
```

Genera `raft_log.txt` en el directorio actual y muestra la salida en pantalla.

### Prototipo de Paxos (~4 segundos)

```bash
cd paxos
python3 paxos_simulacion.py
```

Genera `paxos_log.txt` en el directorio actual.

> **Nota:** los *timeouts* son aleatorios, por lo que **el nodo que resulte electo líder cambia entre ejecuciones**. Es intencional: demuestra que el algoritmo no depende de ninguna configuración privilegiada. Para obtener ejecuciones reproducibles, fije la semilla con `random.seed(42)` dentro de `main()`.

---

## Arquitectura

Cada nodo es un **actor**: un hilo independiente con su propia cola de mensajes (*inbox*). No existe memoria compartida entre nodos; la única forma de comunicarse es enviando mensajes a través de la clase `Red`, que simula:

- **Latencia aleatoria** de 5 a 30 ms por mensaje.
- **Caída de nodos** descartando silenciosamente sus mensajes, de modo que los demás perciben el fallo por *ausencia de respuesta*, tal como ocurre en un sistema real.

Esta decisión de diseño evita interbloqueos y hace fiel la simulación al modelo distribuido.

---

## Prototipo de Raft

**Configuración:** 5 nodos · quórum = 3 · tolera 2 fallos

### Qué implementa

| Componente | Detalle |
|---|---|
| Estados | `SEGUIDOR` · `CANDIDATO` · `LIDER` |
| RPC | `RequestVote` y `AppendEntries` (con latidos) |
| Elección | *Timeouts* aleatorios, un voto por *term*, restricción de log actualizado (§5.4.1) |
| Replicación | Chequeo de consistencia `prevLogIndex`/`prevLogTerm`, retroceso de `nextIndex` |
| Commit | Solo por mayoría y solo para entradas del *term* actual (§5.4.2) |
| Máquina de estados | Almacén clave-valor replicado |

### Escenarios simulados

1. Elección del líder inicial entre los 5 nodos
2. Propuesta y consenso sobre el valor `A=1`
3. Un seguidor rechaza escrituras del cliente (solo el líder las acepta)
4. **Caída del líder** y elección automática de un sucesor en un *term* superior
5. El consenso continúa sin el nodo caído: se acuerda `B=2`
6. **Reincorporación** del nodo caído y puesta al día automática de su log
7. Verificación automática de tres propiedades de seguridad

### Salida esperada (extracto)

```
[  1.128s] [NODO-3   ] [LIDERAZGO] *** ELECTO LIDER del term 1 con 3/5 votos (quorum=3) ***
[  1.583s] [NODO-3   ] [COMMIT   ] *** CONSENSO ALCANZADO *** Entrada #1 'A=1' replicada en 3/5 nodos -> COMPROMETIDA
[  3.556s] [SISTEMA  ] [FALLO    ] !!! SIMULANDO CAIDA DEL LIDER NODO-3 !!!
[  4.378s] [NODO-4   ] [LIDERAZGO] *** ELECTO LIDER del term 2 con 3/5 votos (quorum=3) ***
[  8.166s] [SISTEMA  ] [VERIFICA ] 2. Maquinas de estado identicas en los 5 nodos: OK -> [('A','1'), ('B','2')]
```

Recuperación tras la caída del líder: **~800 ms**, sin intervención humana.

---

## Prototipo de Paxos

**Configuración:** 5 aceptadores · 2 proponentes · 1 aprendiz · quórum = 3

### Qué implementa

| Rol | Responsabilidad |
|---|---|
| **Proponente** | Dirige las fases 1a (`PREPARE`) y 2a (`ACCEPT`); genera números de propuesta únicos |
| **Aceptador** | Responde `PROMISE` o `NACK`; almacena `minProposal`, `acceptedProposal` y `acceptedValue` |
| **Aprendiz** | Detecta cuándo un valor alcanzó la mayoría y quedó *elegido* |

### Escenarios simulados

1. Ronda completa sin fallos: se acuerda `A=1`
2. **Regla de seguridad**: un segundo proponente intenta imponer otro valor y es forzado a adoptar el ya acordado
3. Fallo de 2 aceptadores: el consenso sobrevive con la mayoría viva
4. Fallo de 3 aceptadores: se pierde el quórum y el sistema se detiene **sin corromper datos** (comportamiento CP del teorema CAP)
5. Recuperación de los aceptadores y reconfirmación del valor

### Salida esperada (extracto)

```
[  0.074s] [APRENDIZ ] *** VALOR ELEGIDO: 'A=1' *** aceptado por 3/5 aceptadores (quorum=3)
[  0.988s] [PROPONENTE-1] [SEGURIDAD] REGLA DE SEGURIDAD: ACEPTADOR-5 reporto el valor ya aceptado 'A=1'.
           El proponente ABANDONA su valor 'C=7' y propone 'A=1'.
[  2.440s] [PROPONENTE-1] [ABORTA] FASE 1 FALLIDA: solo 2/3 promesas. Sin mayoria NO se puede avanzar.
```

---

## Propiedades verificadas automáticamente

Al final de cada ejecución los programas comprueban por sí mismos:

**Raft**
1. Los logs comprometidos de todos los nodos vivos son consistentes entre sí
2. Las máquinas de estado de los 5 nodos son idénticas
3. Existe exactamente un líder vigente

**Paxos**
1. Ningún aceptador guarda un valor distinto al elegido
2. El valor elegido permanece inmutable a lo largo de toda la simulación

---

## Ajuste de parámetros

Los tiempos están deliberadamente ampliados para que los logs sean legibles durante la demostración. Para acercarlos a valores de producción, edite las constantes al inicio de `raft/raft_simulacion.py`:

```python
NUM_NODOS = 5                  # Número de nodos del clúster
HEARTBEAT_INTERVAL = 0.25      # Producción: ~0.05 s
ELECCION_TIMEOUT_MIN = 0.80    # Producción: ~0.15 s
ELECCION_TIMEOUT_MAX = 1.60    # Producción: ~0.30 s
```

Aumentar `NUM_NODOS` a 7 eleva la tolerancia a 3 fallos; el quórum se recalcula solo.

---

## Referencias

- Ongaro, D. y Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm*. USENIX ATC '14.
- Lamport, L. (2001). *Paxos Made Simple*. ACM SIGACT News, 32(4).
- Fischer, Lynch y Paterson (1985). *Impossibility of Distributed Consensus with One Faulty Process*. JACM 32(2).
- Visualización interactiva de Raft: https://raft.github.io
