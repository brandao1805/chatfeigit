package br.fei.chat;

import br.fei.chat.proto.Protocol.Envelope;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.file.*;
import java.sql.*;
import java.util.UUID;
import org.zeromq.*;

/** Implementação Java do mesmo contrato usado pelo Python. */
public class Main {
    static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    static final String NODE = env("NODE_ID", "java");
    static long now() { return System.currentTimeMillis(); }
    static void log(String direction, Envelope message) {
        System.out.println("[" + NODE + "] " + direction + " " + message.toString().replace("\n", " | "));
    }
    static boolean valid(String name) { return name.matches("[A-Za-z0-9_-]{1,32}"); }
    static Connection database() throws Exception {
        Path path = Path.of(env("DB_PATH", "/data/chat.db")).toAbsolutePath();
        Files.createDirectories(path.getParent());
        Connection db = DriverManager.getConnection("jdbc:sqlite:" + path);
        try (Statement s = db.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS logins(request_id TEXT PRIMARY KEY, username TEXT NOT NULL, timestamp_ms INTEGER NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS channels(name TEXT PRIMARY KEY, creator TEXT NOT NULL, timestamp_ms INTEGER NOT NULL)");
            s.execute("CREATE TABLE IF NOT EXISTS requests(request_id TEXT PRIMARY KEY, request BLOB NOT NULL, response BLOB NOT NULL)");
        }
        return db;
    }
    static Envelope handle(Connection db, Envelope q) throws Exception {
        var r = Envelope.newBuilder().setRequestId(q.getRequestId()).setTimestampMs(now())
            .setOperation(q.getOperation()).setServerId(NODE);
        if(q.getRequestId().isEmpty() || q.getTimestampMs() <= 0)
            return r.setStatus("INVALID_REQUEST").setDetail("request_id e timestamp_ms positivos são obrigatórios").build();
        try(var s = db.prepareStatement("SELECT request,response FROM requests WHERE request_id=?")) {
            s.setString(1,q.getRequestId());
            try(var rows = s.executeQuery()) {
                if(rows.next()) {
                    if(!Envelope.parseFrom(rows.getBytes(1)).equals(q))
                        return r.setStatus("ID_CONFLICT").setDetail("request_id reutilizado com conteúdo diferente").build();
                    return Envelope.parseFrom(rows.getBytes(2)).toBuilder().setTimestampMs(now()).build();
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
            db.commit();return reply;
        } catch(Exception e) { db.rollback();throw e; }
        finally { db.setAutoCommit(true); }
    }
    static void server() throws Exception {
        try(Connection db=database();ZContext ctx=new ZContext()) {
            ZMQ.Socket socket=ctx.createSocket(SocketType.REP);socket.setLinger(0);
            String bind=System.getenv("BIND_ENDPOINT");
            if(bind!=null) socket.bind(bind);else socket.connect(env("BACKEND_ENDPOINT","tcp://broker:5556"));
            System.out.println("["+NODE+"] servidor pronto");
            while(!Thread.currentThread().isInterrupted()) {
                byte[] raw=socket.recv(0);if(raw==null) break;
                Envelope reply;Envelope q=null;
                try { q=Envelope.parseFrom(raw);log("RECV",q);reply=handle(db,q); }
                catch(InvalidProtocolBufferException e) {
                    System.out.println("["+NODE+"] RECV inválido hex="+java.util.HexFormat.of().formatHex(raw));
                    reply=Envelope.newBuilder().setTimestampMs(now()).setServerId(NODE).setStatus("INVALID_PROTOBUF").setDetail("Mensagem binária inválida").build();
                } catch(SQLException e) {
                    reply=Envelope.newBuilder().setRequestId(q==null?"":q.getRequestId()).setTimestampMs(now()).setServerId(NODE).setStatus("STORAGE_ERROR").setDetail(e.getMessage()).build();
                }
                log("SEND",reply);socket.send(reply.toByteArray(),0);
            }
        }
    }
    static Envelope invoke(ZContext ctx,String op,String channel) throws Exception {
        Envelope q=Envelope.newBuilder().setRequestId(UUID.randomUUID().toString()).setTimestampMs(now())
            .setOperation(op).setUsername(NODE).setChannel(channel).build();
        while(true) {
            ZMQ.Socket s=ctx.createSocket(SocketType.REQ);s.setLinger(0);s.setReceiveTimeOut(3000);
            try {
                s.connect(env("FRONTEND_ENDPOINT","tcp://broker:5555"));log("SEND",q);s.send(q.toByteArray(),0);
                byte[] raw=s.recv(0);
                if(raw!=null) { Envelope r=Envelope.parseFrom(raw);log("RECV",r);if(r.getRequestId().equals(q.getRequestId())) return r; }
            } finally { ctx.destroySocket(s); }
            System.out.println("["+NODE+"] timeout; repetindo operação "+q.getRequestId());Thread.sleep(1000);
        }
    }
    static void bot() throws Exception {
        try(ZContext ctx=new ZContext()) {
            while(!invoke(ctx,"LOGIN","").getStatus().equals("OK")) Thread.sleep(1000);
            invoke(ctx,"CREATE_CHANNEL","geral");invoke(ctx,"CREATE_CHANNEL","canal_"+NODE);
            while(true) { invoke(ctx,"LIST_CHANNELS","");Thread.sleep(3000); }
        }
    }
    public static void main(String[] args) throws Exception {
        switch(args[0]) { case "server" -> server();case "bot" -> bot();default -> throw new IllegalArgumentException("Use server ou bot"); }
    }
}
