package br.fei.chat;
import br.fei.chat.proto.Protocol.Envelope;
import br.fei.chat.proto.Protocol.ServerInfo;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.zeromq.*;

/** Controle entre servidores; cada thread possui seus próprios sockets. */
final class Coordination {
    static volatile String leader="";
    static final AtomicBoolean wake=new AtomicBoolean();
    static Envelope rpc(ZContext ctx,String name,String op,String body) throws Exception {
        ZMQ.Socket s=ctx.createSocket(SocketType.REQ);s.setLinger(0);s.setReceiveTimeOut(700);
        try {
            String address="tcp://"+name+":5560";
            for(String item:Main.env("CONTROL_ENDPOINTS", "").split(",")){String[] pair=item.split("=",2);if(pair.length==2 && pair[0].equals(name))address=pair[1];}
            s.connect(address);
            var q=Main.sending(Envelope.newBuilder().setRequestId(UUID.randomUUID().toString()).setServerId(Main.NODE).setOperation(op).setBody(body).build());
            Main.log("CONTROL SEND",q);s.send(q.toByteArray(),0);
            byte[] raw=s.recv(0);if(raw==null)return null;
            var r=Envelope.parseFrom(raw);Main.received(r);Main.log("CONTROL RECV",r);
            return r.getRequestId().equals(q.getRequestId())&&r.getStatus().equals("OK")?r:null;
        } finally {ctx.destroySocket(s);}
    }
    static void start() {
        Thread listener=new Thread(()->{
            try(ZContext ctx=new ZContext()) {
                var s=ctx.createSocket(SocketType.REP);s.bind(Main.env("CONTROL_BIND","tcp://*:5560"));s.setReceiveTimeOut(50);
                var sub=ctx.createSocket(SocketType.SUB);sub.connect(Main.env("SUB_ENDPOINT","tcp://pubsub:5558"));sub.subscribe("servers".getBytes());
                while(true) {
                    byte[] topic=sub.recv(ZMQ.DONTWAIT);
                    if(topic!=null && sub.hasReceiveMore()) {
                        var q=Envelope.parseFrom(sub.recv(0));Main.received(q);Main.log("COORDINATOR RECV",q);
                        if(new String(topic).equals("servers")&&q.getOperation().equals("COORDINATOR"))leader=q.getServerId();
                    }
                    byte[] raw=s.recv(0);if(raw==null)continue;
                    var q=Envelope.parseFrom(raw);Main.received(q);Main.log("CONTROL RECV",q);
                    var r=Envelope.newBuilder().setRequestId(q.getRequestId()).setServerId(Main.NODE).setOperation(q.getOperation()).setStatus("OK");
                    switch(q.getOperation()) {
                        case "ADJUST" -> {if(q.getServerId().equals(leader)){Main.offset+=Long.parseLong(q.getBody());System.out.println("["+Main.NODE+"] BERKELEY offset_ms="+Main.offset);}else r.setStatus("NOT_COORDINATOR");}
                        case "CLOCK" -> {if(!leader.equals(Main.NODE))r.setStatus("NOT_COORDINATOR");}
                        case "ELECTION", "TIME" -> {}
                        default -> r.setStatus("UNKNOWN_OPERATION");
                    }
                    var response=Main.sending(r.build());Main.log("CONTROL SEND",response);s.send(response.toByteArray(),0);
                }
            } catch(Exception e){e.printStackTrace();}
        });listener.setDaemon(true);listener.start();
        Thread worker=new Thread(()->{
            try(ZContext ctx=new ZContext()) {
                var pub=ctx.createSocket(SocketType.PUB);pub.setLinger(0);pub.connect(Main.env("PUB_ENDPOINT","tcp://pubsub:5557"));
                while(true) {
                    try {
                        var list=Main.referenceCall(ctx,"LIST_SERVERS");
                        if(list!=null) {
                            List<ServerInfo> alive=new ArrayList<>();
                            for(var peer:list.getServersList())if(peer.getName().equals(Main.NODE)||rpc(ctx,peer.getName(),"ELECTION","")!=null)alive.add(peer);
                            if(!alive.isEmpty()) {
                                String winner=alive.stream().max(Comparator.comparingInt(ServerInfo::getRank)).get().getName();
                                if(!winner.equals(leader)){leader=winner;System.out.println("["+Main.NODE+"] ELECTED "+leader);}
                                if(leader.equals(Main.NODE)) {
                                    var q=Main.sending(Envelope.newBuilder().setRequestId(UUID.randomUUID().toString()).setOperation("COORDINATOR").setServerId(Main.NODE).build());
                                    Main.log("COORDINATOR SEND",q);pub.sendMore("servers");pub.send(q.toByteArray(),0);
                                }
                                if(wake.getAndSet(false)) {
                                    if(leader.equals(Main.NODE)) {
                                        Map<String,Long> samples=new LinkedHashMap<>();long sum=0;
                                        for(var peer:alive)if(!peer.getName().equals(Main.NODE)) {
                                            long start=System.nanoTime();var r=rpc(ctx,peer.getName(),"TIME","");
                                            if(r!=null){long delta=r.getTimestampMs()+(System.nanoTime()-start)/2000000-Main.now();samples.put(peer.getName(),delta);sum+=delta;}
                                        }
                                        long mean=sum/(samples.size()+1);
                                        for(var item:samples.entrySet())rpc(ctx,item.getKey(),"ADJUST",Long.toString(mean-item.getValue()));
                                        Main.offset+=mean;System.out.println("["+Main.NODE+"] BERKELEY mean_delta_ms="+mean);
                                    } else {
                                        long start=System.nanoTime();var r=rpc(ctx,leader,"CLOCK","");
                                        if(r!=null)Main.offset=r.getTimestampMs()+(System.nanoTime()-start)/2000000-System.currentTimeMillis();else leader="";
                                    }
                                }
                            }
                        }
                    } catch(Exception e){System.out.println("CONTROL retry: "+e);}
                    Thread.sleep(2000);
                }
            } catch(Exception e){e.printStackTrace();}
        });worker.setDaemon(true);worker.start();
    }
}
