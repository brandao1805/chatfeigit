package br.fei.chat;
import br.fei.chat.proto.Protocol.Envelope;
import br.fei.chat.proto.Protocol.ServerInfo;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.zeromq.*;

/** Controle entre servidores; cada thread possui seus próprios sockets. */
final class Coordination {
    // Estado compartilhado entre a thread que ouve o Pub/Sub e a thread de eleição.
    static volatile String leader="";
    // O servidor principal marca este evento ao completar quinze mensagens de aplicação.
    static final AtomicBoolean wake=new AtomicBoolean();
    static Envelope rpc(ZContext ctx,String name,String op,String body) throws Exception {
        // RPC interno usado para sondagem, leitura de relógio e ajustes Berkeley.
        ZMQ.Socket s=ctx.createSocket(SocketType.REQ);s.setLinger(0);s.setReceiveTimeOut(1500);
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
        // A primeira thread atende pedidos ponto a ponto e escuta anúncios do coordenador.
        Thread listener=new Thread(()->{
            try(ZContext ctx=new ZContext()) {
                var s=ctx.createSocket(SocketType.REP);s.bind(Main.env("CONTROL_BIND","tcp://*:5560"));s.setReceiveTimeOut(50);
                var sub=ctx.createSocket(SocketType.SUB);sub.connect(Main.env("SUB_ENDPOINT","tcp://pubsub:5558"));sub.subscribe("servers".getBytes());
                while(true) {
                    byte[] topic=sub.recv(ZMQ.DONTWAIT);
                    if(topic!=null && sub.hasReceiveMore()) {
                        // O tópico servers comunica o coordenador para todos os servidores inscritos.
                        var q=Envelope.parseFrom(sub.recv(0));Main.received(q);Main.log("COORDINATOR RECV",q);
                        if(new String(topic).equals("servers")&&q.getOperation().equals("COORDINATOR"))leader=q.getServerId();
                    }
                    byte[] raw=s.recv(0);if(raw==null)continue;
                    var q=Envelope.parseFrom(raw);Main.received(q);Main.log("CONTROL RECV",q);
                    var r=Envelope.newBuilder().setRequestId(q.getRequestId()).setServerId(Main.NODE).setOperation(q.getOperation()).setStatus("OK");
                    // Só o líder conhecido pode responder CLOCK/ADJUST como coordenador atual.
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
        // A segunda thread calcula o líder e executa as rodadas de sincronização.
        Thread worker=new Thread(()->{
            // Uma resposta atrasada não deve provocar split-brain. Só removemos
            // um par da eleição depois de duas falhas consecutivas de sondagem.
            Map<String,Integer> misses=new HashMap<>();
            try(ZContext ctx=new ZContext()) {
                var pub=ctx.createSocket(SocketType.PUB);pub.setLinger(0);pub.connect(Main.env("PUB_ENDPOINT","tcp://pubsub:5557"));
                while(true) {
                    try {
                        var list=Main.referenceCall(ctx,"LIST_SERVERS");
                        if(list!=null) {
                            List<ServerInfo> alive=new ArrayList<>();
                            // Uma única falha não exclui um par; isso evita eleição por atraso momentâneo.
                            for(var peer:list.getServersList()) {
                                if(peer.getName().equals(Main.NODE)) {
                                    misses.put(peer.getName(),0);alive.add(peer);
                                } else if(rpc(ctx,peer.getName(),"ELECTION","")!=null) {
                                    misses.put(peer.getName(),0);alive.add(peer);
                                } else {
                                    int count=misses.getOrDefault(peer.getName(),0)+1;
                                    misses.put(peer.getName(),count);
                                    if(count<2)alive.add(peer);
                                }
                            }
                            if(!alive.isEmpty()) {
                                // O maior rank entre os servidores alcançáveis é o vencedor determinístico.
                                String winner=alive.stream().max(Comparator.comparingInt(ServerInfo::getRank)).get().getName();
                                if(!winner.equals(leader)){leader=winner;System.out.println("["+Main.NODE+"] ELECTED "+leader);}
                                if(leader.equals(Main.NODE)) {
                                    // Republica o anúncio para atualizar seguidores que perderam o primeiro evento.
                                    var q=Main.sending(Envelope.newBuilder().setRequestId(UUID.randomUUID().toString()).setOperation("COORDINATOR").setServerId(Main.NODE).build());
                                    Main.log("COORDINATOR SEND",q);pub.sendMore("servers");pub.send(q.toByteArray(),0);
                                }
                                if(wake.getAndSet(false)) {
                                    // A flag agrupa pedidos enquanto a rodada anterior está em andamento.
                                    if(leader.equals(Main.NODE)) {
                                        Map<String,Long> samples=new LinkedHashMap<>();long sum=0;
                                        for(var peer:alive)if(!peer.getName().equals(Main.NODE)) {
                                            // Compensa metade do RTT para aproximar o horário no instante da leitura.
                                            long start=System.nanoTime();var r=rpc(ctx,peer.getName(),"TIME","");
                                            if(r!=null){long delta=r.getTimestampMs()+(System.nanoTime()-start)/2000000-Main.now();samples.put(peer.getName(),delta);sum+=delta;}
                                        }
                                        long mean=sum/(samples.size()+1);
                                        // Corrige cada seguidor e depois aplica ao próprio relógio do líder.
                                        for(var item:samples.entrySet())rpc(ctx,item.getKey(),"ADJUST",Long.toString(mean-item.getValue()));
                                        Main.offset+=mean;System.out.println("["+Main.NODE+"] BERKELEY mean_delta_ms="+mean);
                                    } else {
                                        // O seguidor mede a hora do coordenador sem alterar o relógio do host.
                                        long start=System.nanoTime();var r=rpc(ctx,leader,"CLOCK","");
                                        if(r!=null) {
                                            misses.put(leader,0);
                                            Main.offset=r.getTimestampMs()+(System.nanoTime()-start)/2000000-System.currentTimeMillis();
                                        } else {
                                            int count=misses.getOrDefault(leader,0)+1;
                                            misses.put(leader,count);
                                            if(count>=2)leader="";
                                        }
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
