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
    // A configuração do container substitui o valor padrão sem recompilar o programa.
    static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    static final String NODE = env("NODE_ID", "java");
    // O contador lógico e o ajuste de relógio são compartilhados por todas as threads Java.
    static long clock = 0;
    static volatile long offset = 0;
    static long now() { return System.currentTimeMillis() + offset; }
    static synchronized Envelope sending(Envelope m) {
        // Toda mensagem de saída recebe um novo contador e o timestamp ajustado.
        return m.toBuilder().setLogicalClock(++clock).setTimestampMs(now()).build();
    }
    // Regra de recepção definida no enunciado: avançar até o maior contador observado.
    static synchronized void received(Envelope m) { clock = Math.max(clock, m.getLogicalClock()); }
    static Envelope identity(Envelope m) {
        // Compara a operação ignorando os campos de transporte que mudam em retransmissões.
        return m.toBuilder().clearTimestampMs().clearLogicalClock().build();
    }
    static Envelope referenceCall(ZContext ctx, String operation) throws Exception {
        // Chamada síncrona ao serviço que registra servidores e mantém sua lista ativa.
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
        // A conversão para texto ocorre somente no terminal; no socket segue Protobuf binário.
        System.out.println("[" + NODE + "] " + direction + " " + message.toString().replace("\n", " | "));
    }
    static boolean valid(String name) { return name.matches("[A-Za-z0-9_-]{1,32}"); }
    static Connection database() throws Exception {
        // Cada instância usa seu volume; busy_timeout dá tempo para terminar transações concorrentes.
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
        // Devolve a resposta REQ/REP e, separadamente, o evento que deve ser publicado no Pub/Sub.
        Envelope publication = null;
        var r = Envelope.newBuilder().setRequestId(q.getRequestId()).setTimestampMs(now())
            .setOperation(q.getOperation()).setServerId(NODE);
        if(q.getRequestId().isEmpty() || q.getTimestampMs() <= 0)
            return new Result(r.setStatus("INVALID_REQUEST").setDetail("request_id e timestamp_ms positivos são obrigatórios").build(), null);
        // Requisições repetidas retornam o resultado salvo sem executar a operação de novo neste servidor.
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
            // A transação inclui dados da operação e sua resposta idempotente.
            if(!valid(q.getUsername())) r.setStatus("INVALID_USER").setDetail("Use 1 a 32 letras ASCII, números, _ ou -");
            else switch(q.getOperation()) {
                case "LOGIN" -> {
                    // Registra identidade e timestamp de cada login aceito.
                    try(var s=db.prepareStatement("INSERT INTO logins VALUES (?,?,?)")) {
                        s.setString(1,q.getRequestId());s.setString(2,q.getUsername());s.setLong(3,q.getTimestampMs());s.executeUpdate();
                    }
                    r.setStatus("OK").setDetail("Login registrado");
                }
                case "CREATE_CHANNEL" -> {
                    // A chave primária do canal impede que dois pedidos criem o mesmo nome localmente.
                    if(!valid(q.getChannel())) r.setStatus("INVALID_CHANNEL").setDetail("Nome de canal inválido");
                    else try(var s=db.prepareStatement("INSERT OR IGNORE INTO channels VALUES (?,?,?)")) {
                        s.setString(1,q.getChannel());s.setString(2,q.getUsername());s.setLong(3,q.getTimestampMs());
                        boolean created=s.executeUpdate()>0;
                        r.setStatus(created?"OK":"ALREADY_EXISTS").setDetail(created?"Canal criado":"Canal já existe neste servidor");
                    }
                }
                case "PUBLISH" -> {
                    // Valida tamanho em bytes UTF-8 antes de guardar e publicar o conteúdo.
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
                    // O cursor e o lote usam o mesmo formato da transferência entre réplicas.
                    try {
                        var batch = Replication.export(db, "publications", q.getBody());
                        r.setStatus("OK").setBody(java.util.Base64.getEncoder().encodeToString(batch.toByteArray()));
                    } catch (Exception error) {
                        throw new SQLException("Erro ao consultar histórico", error);
                    }
                }
                case "LIST_CHANNELS" -> {
                    // A ordenação torna a resposta previsível para todos os bots.
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
        } catch(Exception e) { db.rollback();throw e; } // Erro parcial não deixa a operação gravada pela metade.
        finally { db.setAutoCommit(true); }
    }
    static void server() throws Exception {
        // Registra o servidor antes de iniciar os serviços auxiliares e receber clientes.
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
                // O socket REP processa um pedido e então devolve uma resposta ao broker.
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
                    // Só publica depois que handle confirmou o commit local no SQLite.
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
                if (exchanged >= 15) { // Conta pedido, resposta e publicação como mensagens de aplicação.
                    exchanged -= 15; Coordination.wake.set(true);
                }
                if (count % 10 == 0) referenceCall(ctx, "HEARTBEAT");
            }
        }
    }
    static Envelope invoke(ZContext ctx, String op, String channel) throws Exception {
        return invoke(ctx, op, channel, "");
    }

    static Envelope invoke(ZContext ctx, String op, String channel, String body) throws Exception {
        // Bot cliente com UUID estável durante retries para que o servidor reconheça retransmissões.
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
        // Lê publicações durante um segundo sem interromper o ciclo do bot.
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
            // Valida estrutura, inscrição e igualdade do tópico para evitar aceitar prefixos do filtro ZMQ.
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
        // O bot não pede entrada humana: executa login, canais, inscrições e publicação continuamente.
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
                    // Completa a lista de canais até o mínimo previsto no roteiro de testes.
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
                    // Sorteia um canal ainda não inscrito, mantendo no máximo três inscrições.
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
        // O comando do Compose escolhe se este processo atua como bot ou servidor.
        switch(args[0]) { case "server" -> server();case "bot" -> bot();default -> throw new IllegalArgumentException("Use server ou bot"); }
    }
}

