"""ChatFEI: Request-Reply, Publish-Subscribe e persistência."""
import os
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
from chat_pb2 import Envelope

NAME = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
NODE = os.getenv("NODE_ID", "python")

def now():
    return time.time_ns() // 1_000_000

def log(direction, message):
    # Apenas o log é textual; no socket circula SerializeToString().
    print(f"[{NODE}] {direction} {MessageToDict(message, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)}", flush=True)

def database():
    path = os.getenv("DB_PATH", "/data/chat.db")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript("""
    CREATE TABLE IF NOT EXISTS logins(request_id TEXT PRIMARY KEY, username TEXT NOT NULL, timestamp_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS channels(name TEXT PRIMARY KEY, creator TEXT NOT NULL, timestamp_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS requests(request_id TEXT PRIMARY KEY, request BLOB NOT NULL, response BLOB NOT NULL);
    CREATE TABLE IF NOT EXISTS publications(request_id TEXT PRIMARY KEY, channel TEXT NOT NULL, username TEXT NOT NULL, timestamp_ms INTEGER NOT NULL, body TEXT NOT NULL);
    """)
    return db

def handle(db, request):
    reply = Envelope(request_id=request.request_id, timestamp_ms=now(), operation=request.operation, server_id=NODE)
    if not request.request_id or request.timestamp_ms <= 0:
        reply.status, reply.detail = "INVALID_REQUEST", "request_id e timestamp_ms positivos são obrigatórios"
        return reply, None
    old = db.execute("SELECT request,response FROM requests WHERE request_id=?", (request.request_id,)).fetchone()
    if old:
        previous = Envelope.FromString(old[0])
        if previous != request:
            reply.status, reply.detail = "ID_CONFLICT", "request_id reutilizado com conteúdo diferente"
            return reply, None
        cached = Envelope.FromString(old[1])
        cached.timestamp_ms = now()
        return cached, None
    publication = None
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
        elif request.operation == "LIST_CHANNELS":
            reply.status = "OK"
            reply.channels.extend(row[0] for row in db.execute("SELECT name FROM channels ORDER BY name"))
        else:
            reply.status, reply.detail = "UNKNOWN_OPERATION", "Operação desconhecida"
        db.execute("INSERT INTO requests VALUES (?,?,?)", (request.request_id, request.SerializeToString(), reply.SerializeToString()))
    return reply, publication

def server():
    db = database()
    with zmq.Context() as ctx:
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
                raw = socket.recv()
                publication = None
                try:
                    request = Envelope.FromString(raw)
                    log("RECV", request)
                    reply, publication = handle(db, request)
                except DecodeError:
                    print(f"[{NODE}] RECV inválido hex={raw.hex()}", flush=True)
                    reply = Envelope(timestamp_ms=now(), server_id=NODE, status="INVALID_PROTOBUF", detail="Mensagem binária inválida")
                except sqlite3.Error as error:
                    db.rollback()
                    reply = Envelope(request_id=request.request_id, timestamp_ms=now(), server_id=NODE, status="STORAGE_ERROR", detail=str(error))
                if publication is not None:
                    publisher.send_multipart([publication.channel.encode("ascii"), publication.SerializeToString()])
                    log("PUB SEND", publication)
                reply.timestamp_ms = now()
                log("SEND", reply)
                socket.send(reply.SerializeToString())

def call(ctx, request):
    while True:
        # Após timeout recriamos REQ para não violar sua alternância SEND/RECV.
        with ctx.socket(zmq.REQ) as socket:
            socket.linger = 0
            socket.connect(os.getenv("FRONTEND_ENDPOINT", "tcp://broker:5555"))
            log("SEND", request)
            socket.send(request.SerializeToString())
            if socket.poll(3000):
                reply = Envelope.FromString(socket.recv())
                log("RECV", reply)
                if reply.request_id == request.request_id:
                    return reply
        print(f"[{NODE}] timeout; repetindo operação {request.request_id}", flush=True)
        time.sleep(1)

def receive_for(subscriber, subscriptions, seconds):
    deadline = time.monotonic() + seconds
    while (remaining := deadline - time.monotonic()) > 0:
        if not subscriber.poll(max(1, int(remaining * 1000))):
            continue
        frames = subscriber.recv_multipart()
        received_at = now()
        if len(frames) != 2 or frames[0] not in subscriptions:
            continue
        try:
            event = Envelope.FromString(frames[1])
        except DecodeError:
            continue
        if event.operation == "PUBLICATION" and event.channel.encode("utf-8") == frames[0]:
            log(f"SUB RECV received_at_ms={received_at}", event)


def bot():
    with zmq.Context() as ctx, ctx.socket(zmq.SUB) as subscriber:
        subscriber.linger = 0
        subscriber.connect(os.getenv("SUB_ENDPOINT", "tcp://pubsub:5558"))
        subscriptions = set()
        channels = set()

        def invoke(operation, channel="", body=""):
            return call(ctx, Envelope(request_id=str(uuid.uuid4()), timestamp_ms=now(),
                                      operation=operation, username=NODE, channel=channel, body=body))

        while invoke("LOGIN").status != "OK":
            time.sleep(1)
        while True:
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
    with zmq.Context() as ctx, ctx.socket(zmq.XSUB) as xsub, ctx.socket(zmq.XPUB) as xpub:
        xsub.bind(os.getenv("XSUB_BIND", "tcp://*:5557"))
        xpub.bind(os.getenv("XPUB_BIND", "tcp://*:5558"))
        print("Proxy XSUB/XPUB pronto", flush=True)
        zmq.proxy(xsub, xpub)


def broker():
    with zmq.Context() as ctx:
        with ctx.socket(zmq.ROUTER) as frontend, ctx.socket(zmq.DEALER) as backend:
            frontend.bind(os.getenv("BROKER_FRONTEND", "tcp://*:5555"))
            backend.bind(os.getenv("BROKER_BACKEND", "tcp://*:5556"))
            print("Broker ROUTER/DEALER pronto", flush=True)
            zmq.proxy(frontend, backend)

if __name__ == "__main__":
    try:
        {"broker": broker, "server": server, "bot": bot, "pubsub": pubsub}[sys.argv[1]]()
    except KeyboardInterrupt:
        pass
