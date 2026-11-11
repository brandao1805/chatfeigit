"""Anti-entropia pull paginada sobre dados persistidos em SQLite."""
import os, threading, time, uuid, base64
import zmq
from chat_pb2 import Envelope, ReplicaRow, ReplicaBatch
TABLES = {'logins': ['request_id','username','timestamp_ms'],
          'channels': ['name','creator','timestamp_ms'],
          'requests': ['request_id','request','response'],
          'publications': ['request_id','channel','username','timestamp_ms','body']}

def start(app):
    def row(table, values):
        return ReplicaRow(table=table,values=[v.hex() if isinstance(v,bytes) else str(v) for v in values])
    def export(db, table, after):
        if table not in TABLES:raise ValueError('Tabela inválida')
        records=db.execute(f'SELECT * FROM {table} WHERE {TABLES[table][0]} > ? ORDER BY {TABLES[table][0]} LIMIT 129',(after,)).fetchall()
        batch=ReplicaBatch(more=len(records)>128)
        for values in records[:128]:batch.rows.append(row(table,values))
        if batch.rows:batch.next_key=batch.rows[-1].values[0]
        return batch
    def merge(db,batch):
        count=0
        with db:
            db.execute("BEGIN IMMEDIATE")
            for record in batch.rows:
                table=record.table
                if table not in TABLES or len(record.values)!=len(TABLES[table]):raise ValueError('Linha inválida')
                values=list(record.values)
                old=db.execute(f'SELECT * FROM {table} WHERE {TABLES[table][0]}=?',(values[0],)).fetchone()
                if old and row(table,old).SerializeToString().hex()<=record.SerializeToString().hex():continue
                if table=='requests':values[1:]=[bytes.fromhex(v) for v in values[1:]]
                columns=TABLES[table]
                updates=','.join(c+'=excluded.'+c for c in columns[1:])
                db.execute(f'INSERT INTO {table} VALUES ({",".join("?" for _ in values)}) ON CONFLICT({columns[0]}) DO UPDATE SET {updates}',values)
                count+=1
        if count:print(f'[{app.NODE}] REPLICA MERGED {count}',flush=True)
    def listen():
        db=app.database()
        with zmq.Context() as ctx,ctx.socket(zmq.REP) as s:
            s.bind(os.getenv('REPLICA_BIND','tcp://*:5561'))
            while True:
                q=Envelope.FromString(s.recv());app.received(q);app.log("REPLICA RECV",q)
                r=Envelope(request_id=q.request_id,operation='REPLICA',server_id=app.NODE)
                try:
                    batch=export(db,q.channel,q.body)
                    r.body=base64.b64encode(batch.SerializeToString()).decode();r.status='OK'
                except Exception as e:r.status='ERROR';r.detail=str(e)
                app.sending(r);app.log("REPLICA SEND",r);s.send(r.SerializeToString())
    def worker():
        db=app.database()
        endpoints=dict(x.split('=',1) for x in os.getenv('REPLICA_ENDPOINTS','').split(',') if x)
        with zmq.Context() as ctx:
            while True:
                try:
                    listing=app.reference_call(ctx,'LIST_SERVERS')
                    for peer in listing.servers if listing else []:
                        if peer.name==app.NODE:continue
                        for table in TABLES:
                            after=''
                            while True:
                                with ctx.socket(zmq.REQ) as s:
                                    s.linger=0;s.connect(endpoints.get(peer.name,'tcp://'+peer.name+':5561'))
                                    q=app.sending(Envelope(request_id=str(uuid.uuid4()),operation='REPLICA',server_id=app.NODE,channel=table,body=after))
                                    app.log("REPLICA SEND",q);s.send(q.SerializeToString())
                                    if not s.poll(1500):break
                                    r=Envelope.FromString(s.recv());app.received(r);app.log("REPLICA RECV",r)
                                    if r.request_id!=q.request_id or r.status!='OK':break
                                    batch=ReplicaBatch.FromString(base64.b64decode(r.body));merge(db,batch)
                                    if not batch.more:break
                                    after=batch.next_key
                except Exception as e:print(f'[{app.NODE}] REPLICA retry {e}',flush=True)
                time.sleep(2)
    threading.Thread(target=listen,daemon=True).start()
    threading.Thread(target=worker,daemon=True).start()
