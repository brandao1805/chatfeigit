"""Integração Python/Java: eleição, Berkeley e falha do coordenador."""
import os, sys, socket, subprocess, tempfile, time, uuid
from pathlib import Path
import zmq
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'python'))
from chat_pb2 import Envelope

def address():
 with socket.socket() as s:
  s.bind(('127.0.0.1',0));return 'tcp://127.0.0.1:'+str(s.getsockname()[1])
def bind(a):return a.replace('127.0.0.1','*')
with tempfile.TemporaryDirectory() as tmp,zmq.Context() as ctx:
 procs=[];logs={}
 ref,pubin,pubout=address(),address(),address()
 controls={n:address() for n in ['python-server','java-server']}
 apps={n:address() for n in controls}
 mapping=','.join(n+'='+e for n,e in controls.items())
 def start(lang,role,node,**extra):
  log=open(Path(tmp)/(node+'.log'),'w');logs[node]=log
  cmd=[sys.executable,str(ROOT/'python/app.py'),role] if lang=='python' else ['java','-jar',str(ROOT/'java/target/chatfei-1.0.0.jar'),role]
  p=subprocess.Popen(cmd,env={**os.environ,'NODE_ID':node,**extra},stdout=log,stderr=log);procs.append(p);return p
 def rpc(endpoint,op):
  with ctx.socket(zmq.REQ) as s:
   s.linger=0;s.connect(endpoint)
   q=Envelope(request_id=str(uuid.uuid4()),timestamp_ms=int(time.time()*1000),logical_clock=100,operation=op,server_id='test',username='test')
   s.send(q.SerializeToString());assert s.poll(10000),op
   r=Envelope.FromString(s.recv());assert r.request_id==q.request_id;assert r.logical_clock>q.logical_clock
   return r
 def content(n):return Path(tmp,n+'.log').read_text()
 try:
  start('python','reference','reference',REFERENCE_BIND=bind(ref))
  start('python','pubsub','proxy',XSUB_BIND=bind(pubin),XPUB_BIND=bind(pubout))
  servers={}
  for lang in ['python','java']:
   n=lang+'-server'
   servers[n]=start(lang,'server',n,REFERENCE_ENDPOINT=ref,CONTROL_BIND=bind(controls[n]),CONTROL_ENDPOINTS=mapping,PUB_ENDPOINT=pubin,SUB_ENDPOINT=pubout,BIND_ENDPOINT=bind(apps[n]),DB_PATH=str(Path(tmp,n+'.db')))
   assert rpc(apps[n],'LIST_CHANNELS').status=='OK'
  time.sleep(5)
  assert rpc(controls['java-server'],'CLOCK').status=='OK'
  assert rpc(controls['python-server'],'CLOCK').status=='NOT_COORDINATOR'
  print('PASS eleição: Java (maior rank) coordenador, Python seguidor')
  for _ in range(30):
   for n in apps:assert rpc(apps[n],'LIST_CHANNELS').status=='OK'
  time.sleep(5)
  assert 'BERKELEY mean_delta_ms=' in content('java-server')
  assert 'BERKELEY offset_ms=' in content('python-server')
  assert 'COORDINATOR RECV' in content('python-server')
  print('PASS Berkeley: coleta TIME, média, ADJUST e anúncio Pub/Sub entre linguagens')
  servers['java-server'].terminate();servers['java-server'].wait(timeout=5)
  deadline=time.monotonic()+12
  while time.monotonic()<deadline:
   if rpc(controls['python-server'],'CLOCK').status=='OK':break
   time.sleep(1)
  else:raise AssertionError('Python não assumiu após falha')
  print('PASS falha: Python assume após desligamento do coordenador Java')
 except Exception:
  for n in logs:print(n,content(n))
  raise
 finally:
  for p in procs:
   if p.poll() is None:p.terminate()
  for p in procs:
   try:p.wait(timeout=5)
   except subprocess.TimeoutExpired:p.kill();p.wait()
  for log in logs.values():log.close()
