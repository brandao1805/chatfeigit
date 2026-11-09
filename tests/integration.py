"""Integração real: Protobuf/ZeroMQ, Python/Java e recuperação SQLite."""
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
import zmq
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'python'))
from chat_pb2 import Envelope

def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def command(lang, role):
    return [sys.executable, str(ROOT/'python/app.py'), role] if lang=='python' else ['java','-jar',str(ROOT/'java/target/chatfei-1.0.0.jar'),role]

with tempfile.TemporaryDirectory() as tmp, zmq.Context() as ctx:
    for lang in ['python','java']:
        endpoint=f'tcp://127.0.0.1:{port()}'
        db=str(Path(tmp)/f'{lang}.db')
        env={**os.environ,'NODE_ID':f'{lang}-server','DB_PATH':db,'BIND_ENDPOINT':endpoint.replace('127.0.0.1', '*')}
        processes=[]
        with open(Path(tmp)/f'{lang}.log','w+') as log:
            def start():
                p=subprocess.Popen(command(lang,'server'),env=env,stdout=log,stderr=log);processes.append(p);return p
            def rpc(q):
                with ctx.socket(zmq.REQ) as s:
                    s.linger=0;s.connect(endpoint);s.send(q if isinstance(q,bytes) else q.SerializeToString())
                    assert s.poll(15000),f'Timeout no servidor {lang}'
                    return Envelope.FromString(s.recv())
            def msg(op,channel='',user='teste'):
                return Envelope(request_id=str(uuid.uuid4()),timestamp_ms=int(time.time()*1000),operation=op,username=user,channel=channel)
            try:
                p=start()
                login=msg('LOGIN');assert rpc(login).status=='OK';assert rpc(login).status=='OK'
                conflict=Envelope();conflict.CopyFrom(login);conflict.username='outro';assert rpc(conflict).status=='ID_CONFLICT'
                assert rpc(msg('CREATE_CHANNEL','geral')).status=='OK'
                assert rpc(msg('CREATE_CHANNEL','geral')).status=='ALREADY_EXISTS'
                assert rpc(msg('CREATE_CHANNEL','inválido')).status=='INVALID_CHANNEL'
                assert rpc(msg('LOGIN',user='')).status=='INVALID_USER'
                assert rpc(msg('BAD')).status=='UNKNOWN_OPERATION'
                assert rpc(b'\xff').status=='INVALID_PROTOBUF'
                assert list(rpc(msg('LIST_CHANNELS')).channels)==['geral']
                with sqlite3.connect(db) as conn:
                    assert conn.execute('SELECT count(*) FROM logins').fetchone()[0]==1
                p.terminate();p.wait(timeout=5);p=start()
                assert list(rpc(msg('LIST_CHANNELS')).channels)==['geral']
                other='java' if lang=='python' else 'python'
                bot=subprocess.Popen(command(other,'bot'),env={**os.environ,'NODE_ID':f'{other}-bot','FRONTEND_ENDPOINT':endpoint},stdout=log,stderr=log);processes.append(bot)
                deadline=time.monotonic()+15
                found=False
                while time.monotonic()<deadline:
                    with sqlite3.connect(db) as conn:
                        found=conn.execute('SELECT 1 FROM channels WHERE name=?',(f'canal_{other}-bot',)).fetchone() is not None
                    if found:break
                    time.sleep(.2)
                assert found,f'Bot {other} não completou fluxo no servidor {lang}'
                print(f'PASS servidor {lang}: erros, deduplicação, persistência e bot {other}')
            except Exception:
                log.flush();log.seek(0);print(log.read());raise
            finally:
                for p in processes:
                    if p.poll() is None:p.terminate()
                for p in processes:
                    try:p.wait(timeout=5)
                    except subprocess.TimeoutExpired:p.kill();p.wait()
print('PASS integração completa')

# Mesma topologia do Compose, como processos locais: 4 bots + 4 servidores + broker.
with tempfile.TemporaryDirectory() as tmp:
    front=f'tcp://127.0.0.1:{port()}'; back=f'tcp://127.0.0.1:{port()}'
    processes=[];logs=[]
    def launch(lang,role,node,extra):
        log=open(Path(tmp)/(node+'.log'),'w+');logs.append(log)
        p=subprocess.Popen(command(lang,role),env={**os.environ,'NODE_ID':node,**extra},stdout=log,stderr=log)
        processes.append(p)
        return log
    try:
        launch('python','broker','broker',{'BROKER_FRONTEND':front.replace('127.0.0.1', '*'),'BROKER_BACKEND':back.replace('127.0.0.1', '*')})
        for lang in ['python','java']:
            for i in [1,2]:
                node=f'{lang}-server-{i}'
                launch(lang,'server',node,{'BACKEND_ENDPOINT':back,'DB_PATH':str(Path(tmp)/(node+'.db'))})
        botlogs=[]
        for lang in ['python','java']:
            for i in [1,2]:
                botlogs.append(launch(lang,'bot',f'{lang}-bot-{i}',{'FRONTEND_ENDPOINT':front}))
        deadline=time.monotonic()+25
        while time.monotonic()<deadline:
            ready=True
            for log in botlogs:
                log.flush();log.seek(0);text=log.read()
                ready &= ('LIST_CHANNELS' in text and 'RECV' in text and 'server' in text)
            for lang in ['python','java']:
                for i in [1,2]:
                    path=Path(tmp)/f'{lang}-server-{i}.db'
                    try:
                        with sqlite3.connect(path) as db:
                            ready &= db.execute('SELECT count(*) FROM requests').fetchone()[0]>0
                    except sqlite3.OperationalError:
                        ready=False
            if ready:break
            assert all(p.poll() is None for p in processes),'Processo encerrou inesperadamente'
            time.sleep(.2)
        assert ready,'Bots não completaram o fluxo pelo broker'
        for lang in ['python','java']:
            for i in [1,2]:
                path=Path(tmp)/f'{lang}-server-{i}.db'
                with sqlite3.connect(path) as db:
                    assert db.execute('SELECT count(*) FROM requests').fetchone()[0]>0
        print('PASS topologia de 9 processos: broker, 4 bots, 4 servidores e 4 bancos separados')
    finally:
        for p in processes:
            if p.poll() is None:p.terminate()
        for p in processes:
            try:p.wait(timeout=5)
            except subprocess.TimeoutExpired:p.kill();p.wait()
        for log in logs:log.close()
