"""Eleição por maior rank disponível e média de Berkeley."""
import os, time, threading, uuid
import zmq
from chat_pb2 import Envelope

def start(app):
    # Mantém o líder conhecido e sinaliza à thread de controle que é hora do Berkeley.
    wake = threading.Event()
    state = {'leader': ''}
    endpoints = dict(item.split('=',1) for item in os.getenv('CONTROL_ENDPOINTS','').split(',') if item)
    endpoint = lambda name: endpoints.get(name, 'tcp://' + name + ':5560')
    def rpc(ctx, name, op, body=''):
        # RPC ponto a ponto usa um socket REQ novo para cada pedido e resposta.
        with ctx.socket(zmq.REQ) as s:
            s.linger=0;s.connect(endpoint(name))
            q=app.sending(Envelope(request_id=str(uuid.uuid4()),server_id=app.NODE,operation=op,body=body))
            app.log('CONTROL SEND',q);s.send(q.SerializeToString())
            if not s.poll(1500):return None
            r=Envelope.FromString(s.recv());app.received(r);app.log('CONTROL RECV',r)
            return r if r.request_id==q.request_id and r.status=='OK' else None
    def listener():
        # Esta thread recebe anúncios Pub/Sub e atende os pedidos de eleição/relógio.
        with zmq.Context() as ctx, ctx.socket(zmq.REP) as s, ctx.socket(zmq.SUB) as sub:
            s.bind(os.getenv('CONTROL_BIND','tcp://*:5560'));sub.connect(os.getenv('SUB_ENDPOINT','tcp://pubsub:5558'));sub.subscribe(b'servers')
            while True:
                if sub.poll(0):
                    # O tópico servers anuncia qual processo está atuando como líder.
                    frames=sub.recv_multipart()
                    if len(frames)==2 and frames[0]==b'servers':
                        q=Envelope.FromString(frames[1]);app.received(q);app.log('COORDINATOR RECV',q)
                        if q.operation=='COORDINATOR':state['leader']=q.server_id
                if not s.poll(50):continue
                q=Envelope.FromString(s.recv());app.received(q);app.log('CONTROL RECV',q)
                r=Envelope(request_id=q.request_id,server_id=app.NODE,operation=q.operation,status='OK')
                # TIME e ELECTION são consultas; ADJUST e CLOCK só valem para o líder atual.
                if q.operation=='ADJUST':
                    if q.server_id==state['leader']:
                        app.OFFSET+=int(q.body)
                        print(f'[{app.NODE}] BERKELEY offset_ms={app.OFFSET}',flush=True)
                    else:r.status='NOT_COORDINATOR'
                elif q.operation=='CLOCK':
                    if state['leader']!=app.NODE:r.status='NOT_COORDINATOR'
                elif q.operation not in {'ELECTION','TIME'}:r.status='UNKNOWN_OPERATION'
                app.sending(r);app.log('CONTROL SEND',r);s.send(r.SerializeToString())
    def worker():
        # Uma resposta atrasada não deve provocar split-brain. Só removemos
        # um par da eleição depois de duas falhas consecutivas de sondagem.
        misses={}
        with zmq.Context() as ctx, ctx.socket(zmq.PUB) as pub:
            pub.linger=0;pub.connect(os.getenv('PUB_ENDPOINT','tcp://pubsub:5557'))
            while True:
                try:
                    # A referência fornece a mesma lista ordenada por rank a todos os pares.
                    listing=app.reference_call(ctx,'LIST_SERVERS')
                    if listing:
                        alive=[]
                        for peer in listing.servers:
                            # Mantém um peer após uma falha; a segunda falha consecutiva o exclui.
                            if peer.name==app.NODE:
                                misses[peer.name]=0
                                alive.append(peer)
                            elif rpc(ctx,peer.name,'ELECTION'):
                                misses[peer.name]=0
                                alive.append(peer)
                            else:
                                misses[peer.name]=misses.get(peer.name,0)+1
                                if misses[peer.name]<2:alive.append(peer)
                        if alive:
                            # O maior rank entre os pares responsivos vence a eleição.
                            leader=max(alive,key=lambda p:p.rank).name
                            if leader!=state['leader']:
                                state['leader']=leader
                                print(f'[{app.NODE}] ELECTED {leader}',flush=True)
                            if leader==app.NODE:
                                # O anúncio repetido ajuda assinantes que iniciaram depois do PUB.
                                q=app.sending(Envelope(request_id=str(uuid.uuid4()),operation='COORDINATOR',server_id=app.NODE))
                                app.log('COORDINATOR SEND',q);pub.send_multipart([b'servers',q.SerializeToString()])
                            if wake.is_set():
                                # Berkeley mede os desvios e corrige os seguidores em uma rodada.
                                wake.clear()
                                if leader==app.NODE:
                                    samples=[]
                                    for peer in alive:
                                        if peer.name==app.NODE:continue
                                            # Metade do RTT aproxima o instante em que o relógio foi lido.
                                        start=time.monotonic_ns();r=rpc(ctx,peer.name,'TIME')
                                        if r:
                                            half=(time.monotonic_ns()-start)//2000000
                                            samples.append((peer.name,r.timestamp_ms+half-app.now()))
                                    mean=int(sum(d for _,d in samples)/(len(samples)+1))
                                    # Cada servidor recebe sua correção; o líder aplica a própria média.
                                    for name,delta in samples:rpc(ctx,name,'ADJUST',str(mean-delta))
                                    app.OFFSET+=mean
                                    print(f'[{app.NODE}] BERKELEY mean_delta_ms={mean}',flush=True)
                                else:
                                    # O seguidor consulta a hora do líder e tolera falha isolada.
                                    start=time.monotonic_ns();r=rpc(ctx,leader,'CLOCK')
                                    if r:
                                        misses[leader]=0
                                        app.OFFSET=r.timestamp_ms+(time.monotonic_ns()-start)//2000000-time.time_ns()//1000000
                                    else:
                                        misses[leader]=misses.get(leader,0)+1
                                        if misses[leader]>=2:state['leader']=''
                except Exception as e:print(f'[{app.NODE}] CONTROL retry: {e}',flush=True)
                time.sleep(2)
    threading.Thread(target=listener,daemon=True).start()
    threading.Thread(target=worker,daemon=True).start()
    return wake
