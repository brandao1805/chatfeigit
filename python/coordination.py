"""Eleição por maior rank disponível e média de Berkeley."""
import os, time, threading, uuid
import zmq
from chat_pb2 import Envelope

def start(app):
    wake = threading.Event()
    state = {'leader': ''}
    endpoints = dict(item.split('=',1) for item in os.getenv('CONTROL_ENDPOINTS','').split(',') if item)
    endpoint = lambda name: endpoints.get(name, 'tcp://' + name + ':5560')
    def rpc(ctx, name, op, body=''):
        with ctx.socket(zmq.REQ) as s:
            s.linger=0;s.connect(endpoint(name))
            q=app.sending(Envelope(request_id=str(uuid.uuid4()),server_id=app.NODE,operation=op,body=body))
            app.log('CONTROL SEND',q);s.send(q.SerializeToString())
            if not s.poll(700):return None
            r=Envelope.FromString(s.recv());app.received(r);app.log('CONTROL RECV',r)
            return r if r.request_id==q.request_id and r.status=='OK' else None
    def listener():
        with zmq.Context() as ctx, ctx.socket(zmq.REP) as s, ctx.socket(zmq.SUB) as sub:
            s.bind(os.getenv('CONTROL_BIND','tcp://*:5560'));sub.connect(os.getenv('SUB_ENDPOINT','tcp://pubsub:5558'));sub.subscribe(b'servers')
            while True:
                if sub.poll(0):
                    frames=sub.recv_multipart()
                    if len(frames)==2 and frames[0]==b'servers':
                        q=Envelope.FromString(frames[1]);app.received(q);app.log('COORDINATOR RECV',q)
                        if q.operation=='COORDINATOR':state['leader']=q.server_id
                if not s.poll(50):continue
                q=Envelope.FromString(s.recv());app.received(q);app.log('CONTROL RECV',q)
                r=Envelope(request_id=q.request_id,server_id=app.NODE,operation=q.operation,status='OK')
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
        with zmq.Context() as ctx, ctx.socket(zmq.PUB) as pub:
            pub.linger=0;pub.connect(os.getenv('PUB_ENDPOINT','tcp://pubsub:5557'))
            while True:
                try:
                    listing=app.reference_call(ctx,'LIST_SERVERS')
                    if listing:
                        alive=[]
                        for peer in listing.servers:
                            if peer.name==app.NODE or rpc(ctx,peer.name,'ELECTION'):alive.append(peer)
                        if alive:
                            leader=max(alive,key=lambda p:p.rank).name
                            if leader!=state['leader']:
                                state['leader']=leader
                                print(f'[{app.NODE}] ELECTED {leader}',flush=True)
                            if leader==app.NODE:
                                q=app.sending(Envelope(request_id=str(uuid.uuid4()),operation='COORDINATOR',server_id=app.NODE))
                                app.log('COORDINATOR SEND',q);pub.send_multipart([b'servers',q.SerializeToString()])
                            if wake.is_set():
                                wake.clear()
                                if leader==app.NODE:
                                    samples=[]
                                    for peer in alive:
                                        if peer.name==app.NODE:continue
                                        start=time.monotonic_ns();r=rpc(ctx,peer.name,'TIME')
                                        if r:
                                            half=(time.monotonic_ns()-start)//2000000
                                            samples.append((peer.name,r.timestamp_ms+half-app.now()))
                                    mean=int(sum(d for _,d in samples)/(len(samples)+1))
                                    for name,delta in samples:rpc(ctx,name,'ADJUST',str(mean-delta))
                                    app.OFFSET+=mean
                                    print(f'[{app.NODE}] BERKELEY mean_delta_ms={mean}',flush=True)
                                else:
                                    start=time.monotonic_ns();r=rpc(ctx,leader,'CLOCK')
                                    if r:app.OFFSET=r.timestamp_ms+(time.monotonic_ns()-start)//2000000-time.time_ns()//1000000
                                    else:state['leader']=''
                except Exception as e:print(f'[{app.NODE}] CONTROL retry: {e}',flush=True)
                time.sleep(2)
    threading.Thread(target=listener,daemon=True).start()
    threading.Thread(target=worker,daemon=True).start()
    return wake
