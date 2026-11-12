"""ChatFEI: Request-Reply, Publish-Subscribe e persistência."""
import os
import base64
import threading
import random
import re
import sqlite3
import sys
import time
import uuid
from pathlib import Path
import zmq
from google.protobuf.message import DecodeError
from google.protobuf.json_format import MessageToDict
from chat_pb2 import Envelope, ReplicaRow, ReplicaBatch

NAME = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
# NODE identifica este processo nos logs e nas mensagens distribuídas.
NODE = os.getenv("NODE_ID", "python")

# O relógio lógico é compartilhado pelas threads; o lock protege leituras e incrementos.
CLOCK_LOCK = threading.RLock()
CLOCK = 0
# OFFSET ajusta o horário percebido sem alterar o relógio do sistema operacional.
OFFSET = 0

def now():
    # A aplicação troca timestamps em milissegundos; o offset vem do Berkeley.
    return time.time_ns() // 1_000_000 + OFFSET

def sending(message):
    # Toda saída recebe um contador lógico crescente e o horário local ajustado.
    global CLOCK
    with CLOCK_LOCK:
        CLOCK += 1
        message.logical_clock = CLOCK
    message.timestamp_ms = now()
    return message

def received(message):
    # Segue a regra do enunciado: o relógio local nunca fica menor que o recebido.
    global CLOCK
    with CLOCK_LOCK:
        CLOCK = max(CLOCK, message.logical_clock)

def identity(message):
    # Ao deduplicar, ignora apenas os campos de transporte que mudam em retries.
    copy = Envelope()
    copy.CopyFrom(message)
    copy.ClearField("timestamp_ms")
    copy.ClearField("logical_clock")
    return copy

def reference_call(ctx, operation):
    # Canal de controle servidor-referência: cadastro, heartbeat ou lista de pares.
    global OFFSET
    q = Envelope(request_id=str(uuid.uuid4()), operation=operation, server_id=NODE)
    with ctx.socket(zmq.REQ) as sock:
        sock.linger = 0
        sock.connect(os.getenv("REFERENCE_ENDPOINT", "tcp://reference:5559"))
        sending(q)
        log("REF SEND", q)
        start = time.monotonic_ns()
        sock.send(q.SerializeToString())
        if not sock.poll(2000):
            print(f"[{NODE}] referência indisponível; tentará novamente", flush=True)
            return None
        r = Envelope.FromString(sock.recv())
        received(r)
        log("REF RECV", r)
        if r.request_id != q.request_id or r.status != "OK":
            return None
        return r

def reference():
    # Ranks estáveis durante a vida do serviço; disponibilidade expira separadamente.
    ranks, active = {}, {}
    ttl = float(os.getenv("REFERENCE_TTL_SECONDS", "60"))
    with zmq.Context() as ctx, ctx.socket(zmq.REP) as sock:
        sock.linger = 0
        sock.bind(os.getenv("REFERENCE_BIND", "tcp://*:5559"))
        while True:
            # Heartbeats vencidos saem da lista de servidores considerados ativos.
            current = time.monotonic()
            for name in list(active):
                if current - active[name] >= ttl:
                    del active[name]
                    print(f"[reference] EXPIRED {name}", flush=True)
            if not sock.poll(200):
                continue
            raw = sock.recv()
            r = Envelope(server_id=NODE)
            try:
                q = Envelope.FromString(raw)
                received(q)
                log("REF RECV", q)
                r.request_id, r.operation = q.request_id, q.operation
                if not q.request_id or q.timestamp_ms <= 0 or not NAME.fullmatch(q.server_id):
                    r.status = "INVALID_REQUEST"
                elif q.operation in {"REGISTER", "HEARTBEAT"}:
                    # O primeiro cadastro recebe um rank; heartbeat apenas renova presença.
                    if q.server_id not in ranks:
                        ranks[q.server_id] = len(ranks) + 1
                    active[q.server_id] = time.monotonic()
                    r.rank, r.status = ranks[q.server_id], "OK"
                elif q.operation == "LIST_SERVERS":
                    # Um cliente interno recebe apenas os nomes/ranks ainda ativos.
                    r.status = "OK"
                    for name in sorted(active, key=lambda n: ranks[n]):
                        r.servers.add(name=name, rank=ranks[name])
                else:
                    r.status = "UNKNOWN_OPERATION"
            except DecodeError:
                r.status = "INVALID_PROTOBUF"
            sending(r)
            log("REF SEND", r)
            sock.send(r.SerializeToString())

def log(direction, message):
    # Apenas o log é textual; no socket circula SerializeToString().
    print(f"[{NODE}] {direction} {MessageToDict(message, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)}", flush=True)

def database():
    # O volume /data é exclusivo de cada servidor, portanto cada réplica tem seu SQLite.
    path = os.getenv("DB_PATH", "/data/chat.db")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.executescript("""
    CREATE TABLE IF NOT EXISTS logins(request_id TEXT PRIMARY KEY, username TEXT NOT NULL, timestamp_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS channels(name TEXT PRIMARY KEY, creator TEXT NOT NULL, timestamp_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS requests(request_id TEXT PRIMARY KEY, request BLOB NOT NULL, response BLOB NOT NULL);
    CREATE TABLE IF NOT EXISTS publications(request_id TEXT PRIMARY KEY, channel TEXT NOT NULL, username TEXT NOT NULL, timestamp_ms INTEGER NOT NULL, body TEXT NOT NULL);
    """)
    return db

def handle(db, request):
    # Valida e executa uma operação de cliente dentro da transação do servidor.
    reply = Envelope(request_id=request.request_id, timestamp_ms=now(), operation=request.operation, server_id=NODE)
    if not request.request_id or request.timestamp_ms <= 0:
        reply.status, reply.detail = "INVALID_REQUEST", "request_id e timestamp_ms positivos são obrigatórios"
        return reply, None
    # A tabela requests dá idempotência local quando o cliente reenvia o mesmo UUID.
    old = db.execute("SELECT request,response FROM requests WHERE request_id=?", (request.request_id,)).fetchone()
    if old:
        previous = Envelope.FromString(old[0])
        if identity(previous) != identity(request):
            reply.status, reply.detail = "ID_CONFLICT", "request_id reutilizado com conteúdo diferente"
            return reply, None
        cached = Envelope.FromString(old[1])
        cached.timestamp_ms = now()
        return cached, None
    publication = None
    # O contexto commit/rollback mantém a operação e sua resposta juntas no disco.
    with db:
        if not NAME.fullmatch(request.username):
            reply.status, reply.detail = "INVALID_USER", "Use 1 a 32 letras ASCII, números, _ ou -"
        elif request.operation == "LOGIN":
            db.execute("INSERT INTO logins VALUES (?,?,?)", (request.request_id, request.username, request.timestamp_ms))
            reply.status, reply.detail = "OK", "Login registrado"
        elif request.operation == "CREATE_CHANNEL":
            if not NAME.fullmatch(request.channel):
                reply.status, reply.detail = "INVALID_CHANNEL", "Nome de canal inválido"
            else:
                result = db.execute("INSERT OR IGNORE INTO channels VALUES (?,?,?)", (request.channel, request.username, request.timestamp_ms))
                reply.status = "OK" if result.rowcount else "ALREADY_EXISTS"
                reply.detail = "Canal criado" if result.rowcount else "Canal já existe neste servidor"
        elif request.operation == "PUBLISH":
            # Publicação vira registro persistido e, após o commit, evento Pub/Sub.
            if not NAME.fullmatch(request.channel):
                reply.status, reply.detail = "INVALID_CHANNEL", "Nome de canal inválido"
            elif not request.body.strip() or len(request.body.encode("utf-8")) > 4096:
                reply.status, reply.detail = "INVALID_BODY", "Mensagem vazia ou maior que 4096 bytes"
            else:
                publication = Envelope(
                    request_id=request.request_id, timestamp_ms=now(), operation="PUBLICATION",
                    username=request.username, channel=request.channel, body=request.body, server_id=NODE,
                )
                db.execute("INSERT OR IGNORE INTO channels VALUES (?,?,?)",
                           (request.channel, request.username, publication.timestamp_ms))
                db.execute("INSERT INTO publications VALUES (?,?,?,?,?)",
                           (request.request_id, request.channel, request.username,
                            publication.timestamp_ms, request.body))
                reply.status, reply.detail = "OK", "Publicação registrada"
        elif request.operation == "HISTORY":
            # HISTORY usa o mesmo lote Protobuf paginado da replicação.
            records = db.execute("SELECT * FROM publications WHERE request_id > ? ORDER BY request_id LIMIT 129", (request.body,)).fetchall()
            batch = ReplicaBatch(more=len(records) > 128)
            for values in records[:128]:
                batch.rows.append(ReplicaRow(table="publications", values=[str(value) for value in values]))
            if batch.rows:
                batch.next_key = batch.rows[-1].values[0]
            reply.status = "OK"
            reply.body = base64.b64encode(batch.SerializeToString()).decode()
        elif request.operation == "LIST_CHANNELS":
            reply.status = "OK"
            reply.channels.extend(row[0] for row in db.execute("SELECT name FROM channels ORDER BY name"))
        else:
            reply.status, reply.detail = "UNKNOWN_OPERATION", "Operação desconhecida"
        db.execute("INSERT INTO requests VALUES (?,?,?)", (request.request_id, request.SerializeToString(), reply.SerializeToString()))
    return reply, publication

def server():
    # Inicializa dependências de controle antes de aceitar tráfego pelo broker.
    db = database()
    with zmq.Context() as ctx:
        while reference_call(ctx, "REGISTER") is None:
            time.sleep(1)
        reference_call(ctx, "LIST_SERVERS")
        import replication
        replication.start(sys.modules[__name__])
        import coordination
        wake = coordination.start(sys.modules[__name__])
        count = 0
        exchanged = 0
        with ctx.socket(zmq.REP) as socket, ctx.socket(zmq.PUB) as publisher:
            publisher.linger = 0
            publisher.connect(os.getenv("PUB_ENDPOINT", "tcp://pubsub:5557"))
            socket.linger = 0
            if os.getenv("BIND_ENDPOINT"):
                socket.bind(os.environ["BIND_ENDPOINT"])
            else:
                socket.connect(os.getenv("BACKEND_ENDPOINT", "tcp://broker:5556"))
            print(f"[{NODE}] servidor pronto", flush=True)
            while True:
            # REP recebe uma requisição e precisa devolver exatamente uma resposta.
                raw = socket.recv()
                count += 1
                exchanged += 1
                publication = None
                try:
                    request = Envelope.FromString(raw)
                    received(request)
                    log("RECV", request)
                    reply, publication = handle(db, request)
                except DecodeError:
                    print(f"[{NODE}] RECV inválido hex={raw.hex()}", flush=True)
                    reply = Envelope(timestamp_ms=now(), server_id=NODE, status="INVALID_PROTOBUF", detail="Mensagem binária inválida")
                except sqlite3.Error as error:
                    db.rollback()
                    reply = Envelope(request_id=request.request_id, timestamp_ms=now(), server_id=NODE, status="STORAGE_ERROR", detail=str(error))
                if publication is not None:
                    # O evento Pub/Sub só é emitido depois que handle gravou a publicação.
                    exchanged += 1
                    sending(publication)
                    publisher.send_multipart([publication.channel.encode("ascii"), publication.SerializeToString()])
                    log("PUB SEND", publication)
                sending(reply)
                log("SEND", reply)
                socket.send(reply.SerializeToString())
                exchanged += 1
                if exchanged >= 15:
                    # Solicita sincronização Berkeley ao atingir o lote definido no enunciado.
                    exchanged -= 15
                    wake.set()
                if count % 10 == 0:
                    # Renova a presença na referência a cada dez requisições de clientes.
                    reference_call(ctx, "HEARTBEAT")

def call(ctx, request):
    # Cliente REQ: recria o socket a cada retry para preservar a alternância REQ/REP.
    while True:
        # Após timeout recriamos REQ para não violar sua alternância SEND/RECV.
        with ctx.socket(zmq.REQ) as socket:
            socket.linger = 0
            socket.connect(os.getenv("FRONTEND_ENDPOINT", "tcp://broker:5555"))
            sending(request)
            log("SEND", request)
            socket.send(request.SerializeToString())
            if socket.poll(3000):
                reply = Envelope.FromString(socket.recv())
                received(reply)
                log("RECV", reply)
                if reply.request_id == request.request_id:
                    return reply
        print(f"[{NODE}] timeout; repetindo operação {request.request_id}", flush=True)
        time.sleep(1)

def receive_for(subscriber, subscriptions, seconds):
    # Drena mensagens Pub/Sub durante um intervalo sem bloquear a publicação do bot.
    deadline = time.monotonic() + seconds
    while (remaining := deadline - time.monotonic()) > 0:
        if not subscriber.poll(max(1, int(remaining * 1000))):
            continue
        frames = subscriber.recv_multipart()
        received_at = now()
        # O proxy usa o primeiro frame como tópico; o payload Protobuf vem no segundo.
        if len(frames) != 2 or frames[0] not in subscriptions:
            continue
        try:
            event = Envelope.FromString(frames[1])
        except DecodeError:
            continue
        if event.operation == "PUBLICATION" and event.channel.encode("utf-8") == frames[0]:
            received(event)
            log(f"SUB RECV received_at_ms={received_at}", event)


def bot():
    # Bot determinístico: entra, completa canais/inscrições e publica em lotes de dez.
    with zmq.Context() as ctx, ctx.socket(zmq.SUB) as subscriber:
        subscriber.linger = 0
        subscriber.connect(os.getenv("SUB_ENDPOINT", "tcp://pubsub:5558"))
        subscriptions = set()
        channels = set()

        def invoke(operation, channel="", body=""):
            # Cada chamada nova ganha UUID próprio para deduplicação no servidor.
            return call(ctx, Envelope(request_id=str(uuid.uuid4()), timestamp_ms=now(),
                                      operation=operation, username=NODE, channel=channel, body=body))

        while invoke("LOGIN").status != "OK":
            time.sleep(1)
        while True:
            # A lista pode mudar enquanto outros bots criam canais.
            listing = invoke("LIST_CHANNELS")
            if listing.status == "OK":
                channels.update(listing.channels)
                if len(channels) < 5:
                    candidate = next((name for name in ("geral", "estudos", "projetos", "avisos", "cafe")
                                      if name not in channels), None)
                    if candidate and invoke("CREATE_CHANNEL", candidate).status in {"OK", "ALREADY_EXISTS"}:
                        channels.add(candidate)
            if not channels:
                receive_for(subscriber, subscriptions, 1)
                continue
            available = sorted(name for name in channels if name.encode("ascii") not in subscriptions)
            if len(subscriptions) < 3 and available:
                # A escolha aleatória espalha os assinantes pelos tópicos disponíveis.
                topic = random.choice(available)
                encoded = topic.encode("ascii")
                subscriber.subscribe(encoded)
                subscriptions.add(encoded)
                print(f"[{NODE}] SUB inscrito em {topic}", flush=True)
                receive_for(subscriber, subscriptions, 1)
            channel = random.choice(sorted(channels))
            for _ in range(10):
                body = f"Mensagem automática de {NODE}: {uuid.uuid4().hex[:8]}"
                invoke("PUBLISH", channel, body)
                receive_for(subscriber, subscriptions, 1)


def pubsub():
    # Proxy XSUB/XPUB: recebe publicações dos servidores e encaminha aos assinantes.
    with zmq.Context() as ctx, ctx.socket(zmq.XSUB) as xsub, ctx.socket(zmq.XPUB) as xpub:
        xsub.bind(os.getenv("XSUB_BIND", "tcp://*:5557"))
        xpub.bind(os.getenv("XPUB_BIND", "tcp://*:5558"))
        print("Proxy XSUB/XPUB pronto", flush=True)
        zmq.proxy(xsub, xpub)


def broker():
    # Proxy ROUTER/DEALER: aceita clientes e distribui pedidos entre servidores REP.
    with zmq.Context() as ctx:
        with ctx.socket(zmq.ROUTER) as frontend, ctx.socket(zmq.DEALER) as backend:
            frontend.bind(os.getenv("BROKER_FRONTEND", "tcp://*:5555"))
            backend.bind(os.getenv("BROKER_BACKEND", "tcp://*:5556"))
            print("Broker ROUTER/DEALER pronto", flush=True)
            zmq.proxy(frontend, backend)

if __name__ == "__main__":
    # O argumento do container escolhe qual dos quatro processos Python será executado.
    try:
        {"broker": broker, "server": server, "bot": bot, "pubsub": pubsub, "reference": reference}[sys.argv[1]]()
    except KeyboardInterrupt:
        pass

