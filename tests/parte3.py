"""Testes reais da referência, relógios e heartbeat nas duas linguagens."""
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
import zmq
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'python'))
from chat_pb2 import Envelope

def address():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return f'tcp://127.0.0.1:{s.getsockname()[1]}'

def command(lang, role):
    return ([sys.executable, str(ROOT/'python/app.py'), role] if lang == 'python'
            else ['java', '-jar', str(ROOT/'java/target/chatfei-1.0.0.jar'), role])

with tempfile.TemporaryDirectory() as tmp, zmq.Context() as ctx:
    processes, logs = [], []
    def launch(lang, role, name, **extra):
        log = open(Path(tmp)/(name+'.log'), 'w+')
        logs.append(log)
        p = subprocess.Popen(command(lang, role), env={**os.environ, 'NODE_ID':name, **extra}, stdout=log, stderr=log)
        processes.append(p)
        return p
    def rpc(endpoint, operation, name='inspector', logical=1000, request=None):
        q = request or Envelope(request_id=str(uuid.uuid4()), timestamp_ms=int(time.time()*1000),
                                logical_clock=logical, operation=operation, server_id=name, username='teste')
        with ctx.socket(zmq.REQ) as sock:
            sock.linger=0
            sock.connect(endpoint)
            sock.send(q.SerializeToString())
            assert sock.poll(12000), (endpoint, operation, 'timeout')
            r=Envelope.FromString(sock.recv())
            assert r.request_id == q.request_id
            assert r.logical_clock > q.logical_clock
            assert r.timestamp_ms > 0
            return r
    try:
        ref=address()
        launch('python','reference','reference',REFERENCE_BIND=ref.replace("127.0.0.1", "*"),REFERENCE_TTL_SECONDS='3')
        a=rpc(ref,'REGISTER','alpha')
        assert a.rank > 0
        assert rpc(ref,'REGISTER','alpha').rank == a.rank
        b=rpc(ref,'REGISTER','beta')
        assert b.rank != a.rank
        assert len(rpc(ref,'LIST_SERVERS').servers)==2
        time.sleep(3.3)
        assert not rpc(ref,'LIST_SERVERS').servers
        assert rpc(ref,'HEARTBEAT','alpha').rank==a.rank
        print('PASS referência: cadastro único, ranks, listagem, expiração e retorno')
        for lang in ('python','java'):
            endpoint=address()
            node=lang+'-server'
            proc=launch(lang,'server',node,REFERENCE_ENDPOINT=ref,BIND_ENDPOINT=endpoint.replace("127.0.0.1", "*"),
                        PUB_ENDPOINT=address(),DB_PATH=str(Path(tmp)/(node+'.db')))
            for i in range(10):
                r=rpc(endpoint,'LIST_CHANNELS', logical=2000+i*100)
                assert r.status=='OK'
                assert abs(r.timestamp_ms-int(time.time()*1000))<2000
            deadline=time.monotonic()+5
            while time.monotonic()<deadline:
                text=Path(tmp,node+'.log').read_text()
                if 'HEARTBEAT' in text and text.count('CLOCK_SYNC')>=2: break
                time.sleep(.05)
            assert 'HEARTBEAT' in text and text.count('CLOCK_SYNC')>=2
            assert node in [x.name for x in rpc(ref,'LIST_SERVERS').servers]
            q=Envelope(request_id=str(uuid.uuid4()), timestamp_ms=int(time.time()*1000),logical_clock=10000,
                       operation='LOGIN',username='teste')
            assert rpc(endpoint,'LOGIN',request=q).status=='OK'
            q.timestamp_ms+=1;q.logical_clock+=100
            assert rpc(endpoint,'LOGIN',request=q).status=='OK'
            q.username='outro'
            assert rpc(endpoint,'LOGIN',request=q).status=='ID_CONFLICT'
            proc.terminate();proc.wait(timeout=5)
            time.sleep(3.3)
            assert node not in [x.name for x in rpc(ref,'LIST_SERVERS').servers]
            print(f'PASS {lang}: relógio lógico, sincronização física, heartbeat após 10 mensagens, retry e remoção')
    except Exception:
        for log in logs:
            print(Path(log.name).read_text(errors='replace'))
        raise
    finally:
        for p in processes:
            if p.poll() is None:p.terminate()
        for p in processes:
            try:p.wait(timeout=5)
            except subprocess.TimeoutExpired:p.kill();p.wait()
        for log in logs:log.close()
print('PASS Parte 3 completa')
