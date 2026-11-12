"""Convergência de quatro bancos, paginação e recuperação de réplica vazia."""
import base64
import os, sys, socket, subprocess, tempfile, time, uuid, sqlite3
from pathlib import Path
import zmq
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'python'))
from chat_pb2 import Envelope, ReplicaBatch

def address():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0))
        return 'tcp://127.0.0.1:'+str(s.getsockname()[1])
def bind(a):return a.replace('127.0.0.1','*')

with tempfile.TemporaryDirectory() as tmp, zmq.Context() as ctx:
    names=['python-server-1','python-server-2','java-server-1','java-server-2']
    procs=[];logs=[]
    ref,pubin,pubout=address(),address(),address()
    controls={n:address() for n in names};replicas={n:address() for n in names};apps={n:address() for n in names}
    mapping=lambda d:','.join(n+'='+e for n,e in d.items())
    def start(lang,role,node,**extra):
        log=open(Path(tmp)/(node+'.log'),'a');logs.append(log)
        cmd=[sys.executable,str(ROOT/'python/app.py'),role] if lang=='python' else ['java','-jar',str(ROOT/'java/target/chatfei-1.0.0.jar'),role]
        p=subprocess.Popen(cmd,env={**os.environ,'NODE_ID':node,**extra},stdout=log,stderr=log)
        procs.append(p);return p
    def server(n):
        return start(n.split('-')[0],'server',n,REFERENCE_ENDPOINT=ref,CONTROL_BIND=bind(controls[n]),CONTROL_ENDPOINTS=mapping(controls),REPLICA_BIND=bind(replicas[n]),REPLICA_ENDPOINTS=mapping(replicas),PUB_ENDPOINT=pubin,SUB_ENDPOINT=pubout,BIND_ENDPOINT=bind(apps[n]),DB_PATH=str(Path(tmp,n+'.db')))
    def rpc(n,op,body=''):
        with ctx.socket(zmq.REQ) as s:
            s.linger=0;s.connect(apps[n])
            q=Envelope(request_id=str(uuid.uuid4()),timestamp_ms=int(time.time()*1000),logical_clock=100,operation=op,username='teste',channel='geral',body=body)
            s.send(q.SerializeToString());assert s.poll(35000),'timeout '+n
            r=Envelope.FromString(s.recv());assert r.status in {'OK','ALREADY_EXISTS'},r
            return r
    def snapshot(n):
        with sqlite3.connect(Path(tmp,n+'.db'),timeout=30) as db:
            return [db.execute('SELECT * FROM '+t+' ORDER BY 1').fetchall() for t in ['logins','channels','requests','publications']]
    def converge(expected):
        deadline=time.monotonic()+45
        while time.monotonic()<deadline:
            values=[snapshot(n) for n in names]
            if all(v==values[0] for v in values) and len(values[0][3])==expected:return
            time.sleep(.3)
        raise AssertionError('Bancos não convergiram')
    try:
        start('python','reference','reference',REFERENCE_BIND=bind(ref))
        start('python','pubsub','proxy',XSUB_BIND=bind(pubin),XPUB_BIND=bind(pubout))
        servers={}
        for n in names:
            servers[n]=server(n);rpc(n,'LOGIN');rpc(n,'CREATE_CHANNEL')
        for i in range(160):rpc(names[i%4],'PUBLISH','Mensagem '+str(i))
        converge(160)
        print('PASS quatro réplicas iguais: logins, canais, requisições e 160 publicações paginadas',flush=True)
        missing=names[-1];servers[missing].terminate();servers[missing].wait(timeout=5)
        for i in range(20):rpc(names[i%3],'PUBLISH','Durante falha '+str(i))
        Path(tmp,missing+'.db').unlink()
        servers[missing]=server(missing);rpc(missing,'LOGIN')
        converge(180)
        for n in names:
            cursor = ''
            total = 0
            while True:
                response = rpc(n, 'HISTORY', cursor)
                batch = ReplicaBatch.FromString(base64.b64decode(response.body))
                total += len(batch.rows)
                if not batch.more:
                    break
                cursor = batch.next_key
            assert total == 180
        print('PASS consulta paginada do histórico completo nas quatro réplicas', flush=True)
        print('PASS recuperação de banco vazio e mensagens produzidas durante a indisponibilidade',flush=True)
    except Exception:
        for n in names:
            p=Path(tmp,n+'.log')
            if p.exists():print(n,p.read_text(errors='replace')[-6000:])
        raise
    finally:
        for p in procs:
            if p.poll() is None:p.terminate()
        for p in procs:
            try:p.wait(timeout=5)
            except subprocess.TimeoutExpired:p.kill();p.wait()
        for log in logs:log.close()
