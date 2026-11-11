package br.fei.chat;

import br.fei.chat.proto.Protocol.Envelope;
import br.fei.chat.proto.Protocol.ReplicaRow;
import br.fei.chat.proto.Protocol.ReplicaBatch;
import java.sql.*;
import java.util.*;
import org.zeromq.*;

/** Réplica passiva por pull, com páginas de até 128 registros. */
final class Replication {
    static final Map<String,List<String>> TABLES=Map.of(
        "logins",List.of("request_id","username","timestamp_ms"),
        "channels",List.of("name","creator","timestamp_ms"),
        "requests",List.of("request_id","request","response"),
        "publications",List.of("request_id","channel","username","timestamp_ms","body"));

    static ReplicaRow row(String table,ResultSet result) throws SQLException {
        var record=ReplicaRow.newBuilder().setTable(table);
        for(int i=1;i<=TABLES.get(table).size();i++) {
            String value=table.equals("requests")&&i>1
                ? HexFormat.of().formatHex(result.getBytes(i)) : result.getString(i);
            record.addValues(value);
        }
        return record.build();
    }
    static ReplicaBatch export(Connection db,String table,String after) throws Exception {
        if(!TABLES.containsKey(table))throw new IllegalArgumentException("Tabela inválida");
        var batch=ReplicaBatch.newBuilder();
        String key=TABLES.get(table).get(0);
        try(var statement=db.prepareStatement("SELECT * FROM "+table+" WHERE "+key+">? ORDER BY "+key+" LIMIT 129")) {
            statement.setString(1,after);
            try(var rows=statement.executeQuery()) {
                while(rows.next()) {
                    if(batch.getRowsCount()==128){batch.setMore(true);break;}
                    var record=row(table,rows);batch.addRows(record);batch.setNextKey(record.getValues(0));
                }
            }
        }
        return batch.build();
    }
    static void merge(Connection db,ReplicaBatch batch) throws Exception {
        int count=0;
        // Uma transação curta por linha permite atender bots durante páginas grandes.
        for(var record:batch.getRowsList()) {
            String table=record.getTable();var columns=TABLES.get(table);
            if(columns==null||columns.size()!=record.getValuesCount())throw new IllegalArgumentException("Linha inválida");
            // BEGIN IMMEDIATE evita que outro escritor altere a linha entre comparação e atualização.
            try(var begin=db.createStatement()){begin.execute("BEGIN IMMEDIATE");}
            try {
                boolean replace=true;
                try(var statement=db.prepareStatement("SELECT * FROM "+table+" WHERE "+columns.get(0)+"=?")) {
                    statement.setString(1,record.getValues(0));
                    try(var rows=statement.executeQuery()) {
                        if(rows.next())replace=HexFormat.of().formatHex(row(table,rows).toByteArray()).compareTo(HexFormat.of().formatHex(record.toByteArray()))>0;
                    }
                }
                if(replace) {
                    String placeholders=String.join(",",Collections.nCopies(columns.size(),"?"));
                    String updates=String.join(",",columns.subList(1,columns.size()).stream().map(c->c+"=excluded."+c).toList());
                    try(var statement=db.prepareStatement("INSERT INTO "+table+" VALUES ("+placeholders+") ON CONFLICT("+columns.get(0)+") DO UPDATE SET "+updates)) {
                        for(int i=0;i<columns.size();i++) {
                            if(table.equals("requests")&&i>0)statement.setBytes(i+1,HexFormat.of().parseHex(record.getValues(i)));
                            else statement.setString(i+1,record.getValues(i));
                        }
                        statement.executeUpdate();count++;
                    }
                }
                try(var commit=db.createStatement()){commit.execute("COMMIT");}
            } catch(Exception e){try(var rollback=db.createStatement()){rollback.execute("ROLLBACK");}throw e;}
        }
        if(count>0)System.out.println("["+Main.NODE+"] REPLICA MERGED "+count);
    }
    static void start() {
        Thread listener=new Thread(()->{
            try(var db=Main.database();var ctx=new ZContext()) {
                var socket=ctx.createSocket(SocketType.REP);socket.bind(Main.env("REPLICA_BIND","tcp://*:5561"));
                while(true) {
                    var q=Envelope.parseFrom(socket.recv(0));Main.received(q);Main.log("REPLICA RECV",q);
                    var reply=Envelope.newBuilder().setRequestId(q.getRequestId()).setOperation("REPLICA").setServerId(Main.NODE);
                    try {reply.setBody(Base64.getEncoder().encodeToString(export(db,q.getChannel(),q.getBody()).toByteArray())).setStatus("OK");}
                    catch(Exception e){reply.setStatus("ERROR").setDetail(e.toString());}
                    var response=Main.sending(reply.build());Main.log("REPLICA SEND",response);socket.send(response.toByteArray(),0);
                }
            } catch(Exception e){e.printStackTrace();}
        });listener.setDaemon(true);listener.start();
        Thread worker=new Thread(()->{
            Map<String,String> endpoints=new HashMap<>();
            for(String item:Main.env("REPLICA_ENDPOINTS","").split(",")){var pair=item.split("=",2);if(pair.length==2)endpoints.put(pair[0],pair[1]);}
            try(var db=Main.database();var ctx=new ZContext()) {
                while(true) {
                    try {
                        var list=Main.referenceCall(ctx,"LIST_SERVERS");
                        if(list!=null)for(var peer:list.getServersList()) {
                            if(peer.getName().equals(Main.NODE))continue;
                            for(String table:List.of("logins","channels","requests","publications")) {
                                String after="";
                                while(true) {
                                    var socket=ctx.createSocket(SocketType.REQ);socket.setLinger(0);socket.setReceiveTimeOut(1500);
                                    try {
                                        socket.connect(endpoints.getOrDefault(peer.getName(),"tcp://"+peer.getName()+":5561"));
                                        var q=Main.sending(Envelope.newBuilder().setRequestId(UUID.randomUUID().toString()).setServerId(Main.NODE).setOperation("REPLICA").setChannel(table).setBody(after).build());
                                        Main.log("REPLICA SEND",q);socket.send(q.toByteArray(),0);byte[] raw=socket.recv(0);if(raw==null)break;
                                        var reply=Envelope.parseFrom(raw);Main.received(reply);Main.log("REPLICA RECV",reply);
                                        if(!reply.getRequestId().equals(q.getRequestId())||!reply.getStatus().equals("OK"))break;
                                        var batch=ReplicaBatch.parseFrom(Base64.getDecoder().decode(reply.getBody()));merge(db,batch);
                                        if(!batch.getMore())break;after=batch.getNextKey();
                                    } finally {ctx.destroySocket(socket);}
                                }
                            }
                        }
                    } catch(Exception e){System.out.println("REPLICA retry "+e);}
                    Thread.sleep(2000);
                }
            } catch(Exception e){e.printStackTrace();}
        });worker.setDaemon(true);worker.start();
    }
}
