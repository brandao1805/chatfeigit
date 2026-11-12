package br.fei.chat;

import br.fei.chat.proto.Protocol.Envelope;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.zeromq.*;

/** Implementação Java do mesmo contrato usado pelo Python. */
public class Main {
    static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    static final String NODE = env("NODE_ID", "java");
    static long clock = 0;
    static volatile long offset = 0;
    static long now() { return System.currentTimeMillis() + offset; }
    static synchronized Envelope sending(Envelope m) {
        return m.toBuilder().setLogicalClock(++clock).setTimestampMs(now()).build();
    }
    static synchronized void received(Envelope m) { clock = Math.max(clock, m.getLogicalClock()); }
    static Envelope identity(Envelope m) {
        return m.toBuilder().clearTimestampMs().clearLogicalClock().build();
    }
    static Envelope referenceCall(ZContext ctx, String operation) throws Exception {
        ZMQ.Socket sock = ctx.createSocket(SocketType.REQ);
        sock.setLinger(0); sock.setReceiveTimeOut(2000);
        try {
            sock.connect(env("REFERENCE_ENDPOINT", "tcp://reference:5559"));
            Envelope q = sending(Envelope.newBuilder().setRequestId(UUID.randomUUID().toString())
                .setOperation(operation).setServerId(NODE).build());
            log("REF SEND", q);
            long start = System.nanoTime();
            sock.send(q.toByteArray(), 0);
            byte[] raw = sock.recv(0);
            if (raw == null) {
                System.out.println("["+NODE+"] referência indisponível; tentará novamente");
                return null;
            }
            Envelope r = Envelope.parseFrom(raw);
            received(r); log("REF RECV", r);
            if (!r.getRequestId().equals(q.getRequestId()) || !r.getStatus().equals("OK")) return null;
            return r;
        } finally { ctx.destroySocket(sock); }
    }
    static void log(String direction, Envelope message) {
        System.out.println("[" + NODE + "] " + direction + " " + message.toString().replace("\n", " | "));
    }
    static boolean valid(String name) { return name.matches("[A-Za-z0-9_-]{1,32}"); }
    static Connection database() throws Exception {
        Path path = Path.of(env("DB_PATH", "/data/chat.db")).toAbsolutePath();
        Files.createDirectories(path.getParent());
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + path);
        try (Statement s = db.createStatement()) {
            s.execute("PRAGMA busy_timeout=30000");
            s.execute("CREATE TABLE IF NOT EXISTS logins(request_id TEXT PRIMARY KEY, username TEXT NOT NULL, timestamp_ms INTEGER NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS channels(name TEXT PRIMARY KEY, creator TEXT NOT NULL, timestamp_ms INTEGER NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS publications(request_id TEXT PRIMARY KEY, channel TEXT NOT NULL, username TEXT NOT NULL, timestamp_ms INTEGER NOT NULL, body TEXT NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS requests(request_id TEXT PRIMARY KEY, request BLOB NOT NULL, response BLOB NOT NULL)");
        }
        return db;
    }
    record Result(Envelope reply, Envelope publication) {}

    static Result handle(Connection db, Envelope q) throws SQLException, InvalidProtocolBufferException {
        Envelope publication = null;
        var r = Envelope.newBuilder().setRequestId(q.getRequestId()).setTimestampMs(now())
            .setOperation(q.getOperation()).setServerId(NODE);
        if(q.getRequestId().isEmpty() || q.getTimestampMs() <= 0)
            return new Result(r.setStatus("INVALID_REQUEST").setDetail("request_id e timestamp_ms positivos são obrigatórios").build(), null);
        try(var s = db.prepareStatement("SELECT request,response FROM requests WHERE request_id=?")) {
            s.setString(1,q.getRequestId());
            try(var rows = s.executeQuery()) {
                if(rows.next()) {
                    if(!identity(Envelope.parseFrom(rows.getBytes(1))).equals(identity(q)))
                        return new Result(r.setStatus("ID_CONFLICT").setDetail("request_id reutilizado com conteúdo diferente").build(), null);
                    return new Result(Envelope.parseFrom(rows.getBytes(2)), null);
                }
            }
        }
        db.setAutoCommit(false);
        try {
            if(!valid(q.getUsername())) r.setStatus("INVALID_USER").setDetail("Use 1 a 32 letras ASCII, números, _ ou -");
            else switch(q.getOperation()) {
                case "LOGIN" -> {
                    try(var s=db.prepareStatement("INSERT INTO logins VALUES (?,?,?)")) {
                        s.setString(1,q.getRequestId());s.setString(2,q.getUsername());s.setLong(3,q.getTimestampMs());s.executeUpdate();
                    }
                    r.setStatus("OK").setDetail("Login registrado");
                }
                case "CREATE_CHANNEL" -> {
                    if(!valid(q.getChannel())) r.setStatus("INVALID_CHANNEL").setDetail("Nome de canal inválido");
                    else try(var s=db.prepareStatement("INSERT OR IGNORE INTO channels VALUES (?,?,?)")) {
                        s.setString(1,q.getChannel());s.setString(2,q.getUsername());s.setLong(3,q.getTimestampMs());
                        boolean created=s.executeUpdate()>0;
                        r.setStatus(created?"OK":"ALREADY_EXISTS").setDetail(created?"Canal criado":"Canal já existe neste servidor");
                    }
                }
                case "PUBLISH" -> {
                    if (!valid(q.getChannel())) {
                        r.setStatus("INVALID_CHANNEL").setDetail("Nome de canal inválido");
                    } else if (q.getBody().isBlank() || q.getBodyBytes().size() > 4096) {
                        r.setStatus("INVALID_BODY").setDetail("Mensagem vazia ou maior que 4096 bytes");
                    } else {
                        publication = Envelope.newBuilder().setRequestId(q.getRequestId())
                            .setTimestampMs(now()).setOperation("PUBLICATION").setServerId(NODE)
                            .setUsername(q.getUsername()).setChannel(q.getChannel()).setBody(q.getBody()).build();
                        try (var s = db.prepareStatement("INSERT OR IGNORE INTO channels VALUES (?,?,?)")) {
                            s.setString(1, q.getChannel());
                            s.setString(2, q.getUsername());
                            s.setLong(3, publication.getTimestampMs());
                            s.executeUpdate();
                        }
                        try (var s = db.prepareStatement("INSERT INTO publications VALUES (?,?,?,?,?)")) {
                            s.setString(1, q.getRequestId());
                            s.setString(2, q.getChannel());
                            s.setString(3, q.getUsername());
                            s.setLong(4, publication.getTimestampMs());
                            s.setString(5, q.getBody());
                            s.executeUpdate();
                        }
                        r.setStatus("OK").setDetail("Publicação registrada");
                    }
                }
                case "HISTORY" -> {
                    try {
                        var batch = Replication.export(db, "publications", q.getBody());
                        r.setStatus("OK").setBody(java.util.Base64.getEncoder().encodeToString(batch.toByteArray()));
                    } catch (Exception error) {
                        throw new SQLException("Erro ao consultar histórico", error);
                    }
                }
                case "LIST_CHANNELS" -> {
                    r.setStatus("OK");
                    try(var s=db.createStatement();var rows=s.executeQuery("SELECT name FROM channels ORDER BY name")) {
                        while(rows.next()) r.addChannels(rows.getString(1));
                    }
                }
                default -> r.setStatus("UNKNOWN_OPERATION").setDetail("Operação desconhecida");
            }
            Envelope reply=r.build();
            try(var s=db.prepareStatement("INSERT INTO requests VALUES (?,?,?)")) {
                s.setString(1,q.getRequestId());s.setBytes(2,q.toByteArray());s.setBytes(3,reply.toByteArray());s.executeUpdate();
            }
            db.commit();
            return new Result(reply, publication);
        } catch(Exception e) { db.rollback();throw e; }
        finally { db.setAutoCommit(true); }
    }
    static void server() throws Exception {
        try(Connection db=database();ZContext ctx=new ZContext()) {
            while (referenceCall(ctx, "REGISTER") == null) Thread.sleep(1000);
            referenceCall(ctx, "LIST_SERVERS");
            Replication.start();
            Coordination.start();
            long count = 0, exchanged = 0;
            ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);
            publisher.setLinger(0);
            publisher.connect(env("PUB_ENDPOINT", "tcp://pubsub:5557"));
            ZMQ.Socket socket=ctx.createSocket(SocketType.REP);socket.setLinger(0);
            String bind=System.getenv("BIND_ENDPOINT");
            if(bind!=null) socket.bind(bind);else socket.connect(env("BACKEND_ENDPOINT","tcp://broker:5556"));
            System.out.println("["+NODE+"] servidor pronto");
            while(!Thread.currentThread().isInterrupted()) {
                byte[] raw=socket.recv(0);if(raw==null) break;
                count++; exchanged++;
                Envelope reply;
                Envelope q = null;
                Envelope publication = null;
                try {
                    q = Envelope.parseFrom(raw);
                    received(q);
                    log("RECV", q);
                    Result result = handle(db, q);
                    reply = result.reply();
                    publication = result.publication();
                }
                catch(InvalidProtocolBufferException e) {
                    System.out.println("["+NODE+"] RECV inválido hex="+java.util.HexFormat.of().formatHex(raw));
                    reply=Envelope.newBuilder().setTimestampMs(now()).setServerId(NODE).setStatus("INVALID_PROTOBUF").setDetail("Mensagem binária inválida").build();
                } catch(SQLException e) {
                    reply=Envelope.newBuilder().setRequestId(q==null?"":q.getRequestId()).setTimestampMs(now()).setServerId(NODE).setStatus("STORAGE_ERROR").setDetail(e.getMessage()).build();
                }
                if (publication != null) {
                    exchanged++;
                    publication = sending(publication);
                    publisher.sendMore(publication.getChannel());
                    publisher.send(publication.toByteArray(), 0);
                    log("PUB SEND", publication);
                }
                reply = sending(reply);
                log("SEND", reply);
                socket.send(reply.toByteArray(), 0);
                exchanged++;
                if (exchanged >= 15) {exchanged -= 15; Coordination.wake.set(true);}
                if (count % 10 == 0) referenceCall(ctx, "HEARTBEAT");
            }
        }
    }
    static Envelope invoke(ZContext ctx, String op, String channel) throws Exception {
        return invoke(ctx, op, channel, "");
    }

    static Envelope invoke(ZContext ctx, String op, String channel, String body) throws Exception {
        Envelope q=Envelope.newBuilder().setRequestId(UUID.randomUUID().toString()).setTimestampMs(now())
            .setOperation(op).setUsername(NODE).setChannel(channel).setBody(body).build();
        while(true) {
            ZMQ.Socket s=ctx.createSocket(SocketType.REQ);s.setLinger(0);s.setReceiveTimeOut(3000);
            try {
                s.connect(env("FRONTEND_ENDPOINT","tcp://broker:5555"));q=sending(q);log("SEND",q);s.send(q.toByteArray(),0);
                byte[] raw=s.recv(0);
                if(raw!=null) { Envelope r=Envelope.parseFrom(raw);received(r);log("RECV",r);if(r.getRequestId().equals(q.getRequestId())) return r; }
            } finally { ctx.destroySocket(s); }
            System.out.println("["+NODE+"] timeout; repetindo operação "+q.getRequestId());Thread.sleep(1000);
        }
    }
    static void receiveForSecond(ZMQ.Socket subscriber, Set<String> subscriptions) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            subscriber.setReceiveTimeOut((int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            byte[] topicFrame = subscriber.recv(0);
            if (topicFrame == null || !subscriber.hasReceiveMore()) continue;
            byte[] payload = subscriber.recv(0);
            long receivedAt = now();
            boolean extraFrames = subscriber.hasReceiveMore();
            while (subscriber.hasReceiveMore()) subscriber.recv(0);
            String topic = new String(topicFrame, StandardCharsets.US_ASCII);
            if (extraFrames || payload == null || !subscriptions.contains(topic)) continue;
            try {
                Envelope event = Envelope.parseFrom(payload);
                if (event.getOperation().equals("PUBLICATION") && topic.equals(event.getChannel())) {
                    received(event);
                    log("SUB RECV received_at_ms=" + receivedAt, event);
                }
            } catch (InvalidProtocolBufferException ignored) {
                // Um payload inválido não interrompe o bot.
            }
        }
    }

    static void bot() throws Exception {
        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB);
            subscriber.setLinger(0);
            subscriber.connect(env("SUB_ENDPOINT", "tcp://pubsub:5558"));
            Set<String> subscriptions = new HashSet<>();
            Set<String> channels = new TreeSet<>();
            while (!invoke(ctx, "LOGIN", "").getStatus().equals("OK")) Thread.sleep(1000);
            while (true) {
                Envelope listing = invoke(ctx, "LIST_CHANNELS", "");
                if (listing.getStatus().equals("OK")) {
                    channels.addAll(listing.getChannelsList());
                    if (channels.size() < 5) {
                        for (String candidate : List.of("geral", "estudos", "projetos", "avisos", "cafe")) {
                            if (channels.contains(candidate)) continue;
                            String status = invoke(ctx, "CREATE_CHANNEL", candidate).getStatus();
                            if (status.equals("OK") || status.equals("ALREADY_EXISTS")) channels.add(candidate);
                            break;
                        }
                    }
                }
                if (channels.isEmpty()) {
                    receiveForSecond(subscriber, subscriptions);
                    continue;
                }
                List<String> available = channels.stream().filter(name -> !subscriptions.contains(name)).toList();
                if (subscriptions.size() < 3 && !available.isEmpty()) {
                    String topic = available.get(ThreadLocalRandom.current().nextInt(available.size()));
                    subscriber.subscribe(topic.getBytes(StandardCharsets.US_ASCII));
                    subscriptions.add(topic);
                    System.out.println("[" + NODE + "] SUB inscrito em " + topic);
                    receiveForSecond(subscriber, subscriptions);
                }
                List<String> knownChannels = new ArrayList<>(channels);
                String channel = knownChannels.get(ThreadLocalRandom.current().nextInt(knownChannels.size()));
                for (int i = 0; i < 10; i++) {
                    String body = "Mensagem automática de " + NODE + ": " + UUID.randomUUID().toString().substring(0, 8);
                    invoke(ctx, "PUBLISH", channel, body);
                    receiveForSecond(subscriber, subscriptions);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        switch(args[0]) { case "server" -> server();case "bot" -> bot();default -> throw new IllegalArgumentException("Use server ou bot"); }
    }
}

