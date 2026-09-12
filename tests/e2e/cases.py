"""P0/P1/P2 end-to-end cases with API, database and supplier observations."""
import base64
from concurrent.futures import ThreadPoolExecutor
import copy
import datetime
import hashlib
import itertools
import json
import subprocess
import time
import uuid

from harness import COMPOSE, ROOT, compose, database, eventually, http, rabbit, sqlquote

CASES=[]


def case(identifier, title, group):
    def register(fn):
        CASES.append({"id":identifier,"priority":identifier.split("-")[0],"title":title,"group":group,"run":fn})
        return fn
    return register


def error(result,status,code):
    actual,value,headers=result
    assert actual==status,(actual,value)
    assert value["error"]["code"]==code,value
    assert value["error"]["request_id"]=={k.lower():v for k,v in headers.items()}["x-request-id"]


def count(h,identifier):
    return int(database("SELECT COUNT(*) FROM delivery_attempt WHERE notification_id=UNHEX("+sqlquote(identifier.replace("-",""))+")"))


def gap(task):
    rows=list(reversed(task["attempts"]))
    return (datetime.datetime.fromisoformat(rows[1]["started_at"].replace("Z","+00:00"))-datetime.datetime.fromisoformat(rows[0]["finished_at"].replace("Z","+00:00"))).total_seconds()


@case("P0-01","Two instances, durable topology and DB privilege boundaries","foundation")
def foundation(h):
    values=h.health();assert all(d["business_db"] and d["redis"] and d["rabbitmq"] for d in values)
    queues=rabbit("queues/%2F");assert len(queues)==9 and all(q["durable"] and q["type"]=="quorum" for q in queues)
    consumers=rabbit("consumers/%2F");assert len(consumers)==2 and all(c["ack_required"] for c in consumers)
    assert database("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='notify_business'")=="4"
    denied=database("UPDATE config_revision SET revision=revision", "control",False)
    assert denied.returncode and "denied" in denied.stderr.lower()
    denied=database("SELECT * FROM notify_control.config_revision","business",False)
    assert denied.returncode and "denied" in denied.stderr.lower()
    return {"instances":values,"queues":[q["name"] for q in queues]}


@case("P0-02","Durable acceptance, cross-instance query and nullable reports","admission")
def durable(h):
    identifier,key,_=h.accept();task=h.wait(identifier)
    assert task["attempt_count"]==1 and count(h,identifier)==1
    assert h.task(identifier,0)==h.task(identifier,1)
    assert len(h.records(key)["requests"])==1
    assert "report" not in task["attempts"][0] and "request" not in task
    return task


@case("P0-03","Concurrent idempotency and immutable request conflict","admission")
def idempotency(h):
    key,body=h.body()
    with ThreadPoolExecutor(max_workers=8) as pool:rows=list(pool.map(lambda i:h.submit(key,body,i%2),range(8)))
    assert all(r[0] in (200,202) for r in rows),rows
    identifiers={r[1]["notification_id"] for r in rows};assert len(identifiers)==1
    identifier=identifiers.pop();h.wait(identifier)
    assert count(h,identifier)==1 and len(h.records(key)["requests"])==1
    changed=copy.deepcopy(body);changed["request"]["body"]="{}"
    error(h.submit(key,changed,1),409,"IDEMPOTENCY_CONFLICT")
    return {"id":identifier,"concurrent_replays":len(rows)}


@case("P0-04","JWT claims, algorithm, ownership and Target binding","security")
def auth(h):
    error(h.call("notifications",headers={}),401,"UNAUTHORIZED")
    for patch in ({"exp":int(time.time())-10},{"exp":None},{"nbf":int(time.time())+120},{"iss":"foreign"},{"aud":"foreign"},{"sub":""},{"client_id":"missing"}):
        error(h.call("notifications",headers={"Authorization":"Bearer "+h.token(changes=patch)}),401,"UNAUTHORIZED")
    error(h.call("notifications",headers={"Authorization":"Bearer "+h.token(algorithm="HS256")}),401,"UNAUTHORIZED")
    identifier,_,_=h.accept();h.wait(identifier)
    error(h.call("notifications/"+identifier,client="other"),404,"NOTIFICATION_NOT_FOUND")
    error(h.call("notifications/"+identifier+":retry","POST",client="other"),403,"FORBIDDEN")
    doc=copy.deepcopy(h.document);doc["clients"][h.names["other"]]["targets"]=[];h.config(doc)
    try:
        key,body=h.body();error(h.submit(key,body,client="other"),403,"TARGET_NOT_ALLOWED")
    finally:
        doc["clients"][h.names["other"]]["targets"]=h.document["clients"][h.names["client"]]["targets"];h.config(doc)


@case("P0-05","Partial batches remain independent and replay-safe","admission")
def batch(h):
    key,body=h.body();item={**body,"idempotency_key":key}
    invalid={**item,"target":"missing","idempotency_key":key+"-bad"}
    code,data,_=h.call("notifications:batch","POST",{"items":[item,invalid,42]},instance=1)
    assert code==202 and [r["accepted"] for r in data["results"]]==[True,False,False],data
    identifier=data["results"][0]["notification_id"];h.ids.append(identifier);h.wait(identifier)
    code,replay,_=h.call("notifications:batch","POST",{"items":[item]})
    assert code==202 and replay["results"][0]["notification_id"]==identifier
    assert h.task(identifier)["batch_id"]==data["batch_id"]
    assert count(h,identifier)==1
    return data


@case("P0-06","Default HTTP success, terminal and transient classification","delivery")
def statuses(h):
    tasks=[]
    for status in (200,299,400,404,408,429,500,503):
        identifier,_,_=h.accept(statuses=str(status)+",200")
        expected="failed" if status in (400,404) else "delivered"
        tasks.append((identifier,status,expected))
    for identifier,status,expected in tasks:
        task=h.wait(identifier,expected);assert task["attempt_count"]==(2 if status in (408,429,500,503) else 1),task
        assert task["attempts"][-1]["http_status"]==status
    return {"statuses":[status for _,status,_ in tasks]}


@case("P0-07","Proxy success, terminal and retry share persistence fencing","delivery")
def proxy(h):
    result=[]
    for response,expected,attempts in (("200","delivered",1),("400","failed",1),("503,200","delivered",2)):
        identifier,key,_=h.accept(statuses=response,mode="proxy",instance=1)
        task=h.wait(identifier,expected);assert task["attempt_count"]==attempts
        assert len(h.records(key)["requests"])==attempts
        result.append(task)
    return result


@case("P0-08","MQ outage preserves acceptance and reconnects automatically","fault")
def mq_outage(h):
    try:
        compose("stop","rabbitmq")
        eventually(lambda:all(http(f"http://127.0.0.1:{p}/health/ready")[0]==503 for p in (8081,8083)))
        identifier,_,_=h.accept();task=h.task(identifier)
        assert task["status"]=="pending" and task["attempt_count"]==0
    finally:h.start("rabbitmq")
    h.healthy();return h.wait(identifier)


@case("P0-09","Database outage cannot return false durable acceptance","fault")
def db_outage(h):
    key,body=h.body()
    try:
        compose("stop","mariadb")
        error(h.submit(key,body),503,"DEPENDENCY_UNAVAILABLE")
        for port in (8081,8083):
            assert http(f"http://127.0.0.1:{port}/health/live")[0]==200
            assert http(f"http://127.0.0.1:{port}/health/ready")[0]==503
    finally:h.start("mariadb")
    h.healthy();assert database("SELECT COUNT(*) FROM notification_task WHERE client_idem_key="+sqlquote(key))=="0"
    code,value,_=h.submit(key,body);assert code==202;return h.wait(value["notification_id"])


@case("P0-10","Redis fail-open continues through authoritative DB leases","fault")
def redis_open(h):
    try:
        compose("stop","redis")
        values=h.health();assert all(not v["redis"] and v["quota_degraded"] for v in values)
        identifier,_,_=h.accept();data=h.wait(identifier);assert data["attempt_count"]==1
    finally:h.start("redis")
    h.healthy();return data


@case("P0-11","Killed claimant recovers with unknown Attempt and new generation","fault")
def kill_lease(h):
    identifier,key,_=h.accept(delays_ms="3000,0")
    def owner():
        if h.task(identifier)["status"]!="in_flight":return None
        for service,port in (("notifier",8081),("notifier-2",8083)):
            if http(f"http://127.0.0.1:{port}/health/detail")[1]["worker_active"]:return service
    victim=eventually(owner,10)
    try:compose("kill","-s","SIGKILL",victim)
    finally:h.start(victim)
    h.healthy();task=h.wait(identifier,timeout=105)
    assert task["attempt_count"]==2 and task["dispatch_generation"]>1
    assert task["attempts"][1]["result"]=="unknown" and task["attempts"][1]["error_code"]=="lease_expired"
    assert len(h.records(key)["requests"])==2
    return {"victim":victim,"task":task}


@case("P0-12","Concurrent controlled retries cannot duplicate a retry cycle","state")
def manual(h):
    identifier,key,_=h.accept(statuses="400,200",delays_ms="0,800")
    old=h.wait(identifier,"failed")
    with ThreadPoolExecutor(max_workers=4) as pool:rows=list(pool.map(lambda i:h.call("notifications/"+identifier+":retry","POST",instance=i%2),range(4)))
    assert sorted(r[0] for r in rows)==[202,409,409,409],rows
    new=h.wait(identifier);assert new["attempt_count"]==2 and new["dispatch_generation"]==old["dispatch_generation"]+1
    assert all(r["notification"]==identifier for r in h.records(key)["requests"])
    error(h.call("notifications/"+identifier+":retry","POST"),409,"INVALID_STATE")
    return new


@case("P1-01","CEL success/retry/fail/report decisions persist structured reports","hook")
def cel(h):
    tasks=[]
    for target,codes,status,expected,attempts in (("hook","0","503","delivered",1),("hook","1001,0","200","delivered",2),("hook","2000","200","failed",1),("report","0","400","failed",1)):
        identifier,_,_=h.accept(target,statuses=status,codes=codes);tasks.append((identifier,expected,attempts))
    for identifier,expected,attempts in tasks:
        task=h.wait(identifier,expected);assert task["attempt_count"]==attempts and "report" in task["attempts"][0]
    return [h.task(i) for i,_,_ in tasks]


@case("P1-02","Retry sequence, deadline and uncertain side effects are bounded","retry")
def retry_budget(h):
    records=[]
    for target,kwargs,attempts,reason in (("header",{"statuses":"503"},2,"retry_exhausted"),("deadline",{"statuses":"503"},1,"retry_deadline_exceeded"),("no-retry",{"statuses":"503"},1,"automatic_retry_disabled"),("unknown",{"resets":"1"},1,"uncertain_retry_disabled")):
        identifier,_,_=h.accept(target,**kwargs);records.append((identifier,attempts,reason))
    for identifier,attempts,reason in records:
        task=h.wait(identifier,"failed");assert task["attempt_count"]==attempts and task["last_error_code"]==reason,task
    identifier,_,_=h.accept("uncertain",resets="1");task=h.wait(identifier);assert task["attempt_count"]==2
    return {"terminal_policies":len(records),"explicit_uncertain_retry":task}


@case("P1-03","Retry-After seconds/date/invalid/extreme obey not-before and bounds","retry")
def retry_after(h):
    date=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=4)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    tasks=[]
    for value in ("5",date,"not-a-date","999999999"):
        identifier,_,_=h.accept(statuses="429,200",retry_after=value);tasks.append((identifier,value))
    result=[]
    for identifier,value in tasks:
        task=h.wait(identifier,"failed" if value=="999999999" else "delivered")
        if value=="999999999":assert task["attempt_count"]==1
        else:assert task["attempt_count"]==2 and gap(task)>=5
        result.append(task)
    return result


@case("P1-04","Header/query/nested-array JSON Pointer injection is stable","idempotency")
def injection(h):
    for target in ("header","query","body"):
        identifier,key,_=h.accept(target,statuses="503,200");h.wait(identifier)
        rows=h.records(key)["requests"];assert len(rows)==2
        for row in rows:
            injected=row["header_key"] if target=="header" else row["query_key"] if target=="query" else row["body"]["items"][0]["request_id"]
            assert injected==key and row["notification"]==identifier


@case("P1-05","Normalized method/header and equivalent body encodings replay","idempotency")
def normalize(h):
    identifier,key,body=h.accept();h.wait(identifier)
    body["request"]["method"]="post"
    body["request"]["headers"]={"content-type":"application/json"}
    body["request"]["body_encoding"]="base64"
    body["request"]["body"]=base64.b64encode(body["request"]["body"].encode()).decode()
    code,value,_=h.submit(key,body,1);assert code==200 and value["notification_id"]==identifier
    assert count(h,identifier)==1


@case("P1-06","Ingress and egress quotas are shared between instances","quota")
def quotas(h):
    # Enter a Redis fixed window away from its boundary to avoid a valid
    # two-window admission being mislabeled as a quota violation.
    server=compose("exec","-T","redis","redis-cli","--raw","TIME").splitlines()
    fraction=int(server[1])/1_000_000
    time.sleep((1-fraction)+.15)
    pairs=[h.body() for _ in range(2)]
    with ThreadPoolExecutor(max_workers=2) as pool:rows=list(pool.map(lambda i:h.submit(*pairs[i],instance=i,client="limited"),range(2)))
    assert sorted(r[0] for r in rows)==[202,429],rows
    accepted=[r[1]["notification_id"] for r in rows if r[0]==202][0]
    eventually(lambda: h.task(accepted,client="limited")["status"]=="delivered")
    jobs=[h.accept("rate",instance=i%2) for i in range(3)]
    for identifier,_,_ in jobs:assert h.wait(identifier)["attempt_count"]==1
    times=[h.records(key)["requests"][0]["at"][:19] for _,key,_ in jobs]
    assert len(set(times))==3,times
    return {"egress_seconds":times}


@case("P1-07","Async and proxy share distributed target concurrency","quota")
def concurrency(h):
    pairs=[h.body("serial",mode="proxy" if i%2 else "async",delays_ms=300) for i in range(6)]
    with ThreadPoolExecutor(max_workers=6) as pool:rows=list(pool.map(lambda i:h.submit(*pairs[i],instance=i%2),range(6)))
    assert all(r[0] in (200,202) for r in rows)
    for row in rows:assert h.wait(row[1]["notification_id"])["attempt_count"]==1
    active=[r["active"] for key,_ in pairs for r in h.records(key)["requests"]]
    assert max(active)==1,active
    return {"supplier_observed_active":active}


@case("P1-08","Fail-closed denies ingress and postpones persisted delivery","fault")
def redis_closed(h):
    original=copy.deepcopy(h.document);doc=copy.deepcopy(original)
    doc["quota_policies"][h.names["closed"]]["fail_mode"]="closed"
    doc["targets"][h.names["closed"]]["quota_policy"]=h.names["closed"]
    doc["clients"][h.names["limited"]]["quota_policy"]=h.names["closed"]
    h.config(doc)
    try:
        compose("stop","redis")
        assert all(not d["redis"] for d in h.health(False))
        key,body=h.body();error(h.submit(key,body,client="limited"),503,"DEPENDENCY_UNAVAILABLE")
        identifier,key,_=h.accept("closed")
        time.sleep(1);task=h.task(identifier)
        assert task["status"]=="pending" and task["attempt_count"]==0 and not h.records(key)["requests"]
    finally:h.start("redis");h.healthy()
    try:task=h.wait(identifier)
    finally:h.config(original)
    return task


@case("P1-09","Latest security revocation blocks previously scheduled retries","config")
def security_revision(h):
    original=copy.deepcopy(h.document)
    identifier,key,_=h.accept(statuses="503,200")
    eventually(lambda:h.task(identifier)["attempt_count"]==1 and h.task(identifier)["status"]=="pending",10)
    doc=copy.deepcopy(original);doc["targets"][h.names["header"]]["enabled"]=False;h.config(doc)
    try:
        key2,body=h.body();error(h.submit(key2,body),403,"TARGET_DISABLED")
        task=h.wait(identifier,"failed");assert len(h.records(key)["requests"])==1
    finally:h.config(original)
    return task


@case("P1-10","Historical Hook behavior is pinned while new tasks use new revision","config")
def pinned(h):
    original=copy.deepcopy(h.document)
    identifier,key,_=h.accept("hook",codes="1001,0")
    eventually(lambda:h.task(identifier)["attempt_count"]==1 and h.task(identifier)["status"]=="pending",10)
    old=h.task(identifier)["config_revision"]
    doc=copy.deepcopy(original);doc["hooks"][h.names["hook"]]["expression"]="{'action':'fail','reason':'new_policy'}";revision=h.config(doc)
    try:
        previous=h.wait(identifier);assert previous["config_revision"]==old
        newer,_,_=h.accept("hook");current=h.wait(newer,"failed")
        assert current["config_revision"]==revision and current["last_error_code"]=="new_policy"
    finally:h.config(original)
    return {"old_revision":old,"new_revision":revision}


@case("P1-11","Stale/poison messages and confirmed-but-lost trigger recovery","mq")
def messages(h):
    identifier,key,_=h.accept();h.wait(identifier)
    h.publish(json.dumps({"schema_version":1,"event_id":str(uuid.uuid4()),"notification_id":identifier,"generation":999,"event_type":"dispatch","created_at":datetime.datetime.now(datetime.timezone.utc).isoformat()}))
    h.publish("not-valid-json")
    def dead():
        rows=h.queue_get("notify.dead.q")
        if rows:assert rows[0]["payload"]=="not-valid-json";return True
    eventually(dead,10);assert count(h,identifier)==1 and len(h.records(key)["requests"])==1
    lost,key,_=h.accept(statuses="503,200")
    eventually(lambda:h.task(lost)["attempt_count"]==1 and h.task(lost)["status"]=="pending",10)
    def intercept():
        rows=h.queue_get("notify.retry.5s.q")
        if not rows:return None
        payload=json.loads(rows[0]["payload"])
        assert payload["notification_id"]==lost,"suite requires an otherwise idle delay queue"
        return payload
    payload=eventually(intercept,4)
    task=h.wait(lost,timeout=100)
    assert task["attempt_count"]==2 and task["dispatch_generation"]>payload["generation"]
    return task


@case("P1-12","Expired publishing lease is reclaimed without task loss","outbox")
def outbox(h):
    try:
        compose("stop","rabbitmq")
        identifier,_,_=h.accept()
        # Fault injection models a publisher that died after lease acquisition.
        # It does not mutate Notification status, generation or HTTP outcomes.
        database("UPDATE mq_outbox SET status='publishing',publish_token=UNHEX('"+uuid.uuid4().hex+"'),lease_until=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 3 SECOND) WHERE notification_id=UNHEX('"+identifier.replace("-","")+"')")
    finally:h.start("rabbitmq")
    h.healthy();task=h.wait(identifier);assert task["attempt_count"]==1
    return task


@case("P2-01","Body limits include equivalent encoding and oversized whitespace","boundary")
def body_limits(h):
    key,body=h.body();body["request"]["body"]="x"*(1<<20)
    code,value,_=h.submit(key,body);assert code==202;h.wait(value["notification_id"])
    for length,status in (((1<<20)+1,400),((2<<20)+100,413)):
        key,body=h.body();body["request"]["body"]="x"*length
        error(h.submit(key,body),status,"INVALID_REQUEST")
    key,body=h.body();raw=json.dumps(body).encode()+b" "*(2<<20)
    error(h.call("notifications","POST",raw=raw,headers={"Authorization":"Bearer "+h.token(),"Content-Type":"application/json","Idempotency-Key":key}),413,"INVALID_REQUEST")
    assert database("SELECT COUNT(*) FROM notification_task WHERE client_idem_key="+sqlquote(key))=="0"


@case("P2-02","Batch size 0/100/101 boundaries","boundary")
def batch_limits(h):
    for length in (0,101):
        error(h.call("notifications:batch","POST",{"items":[{}]*length}),400,"INVALID_REQUEST")
    items=[]
    for _ in range(100):key,body=h.body();items.append({**body,"idempotency_key":key})
    code,value,_=h.call("notifications:batch","POST",{"items":items})
    assert code==202 and len(value["results"])==100 and all(r["accepted"] for r in value["results"]),value
    ids=[r["notification_id"] for r in value["results"]];h.ids.extend(ids)
    for identifier in ids:assert h.wait(identifier)["attempt_count"]==1
    return {"accepted_items":len(ids)}


@case("P2-03","Invalid JSON, fields, headers, methods and pagination reject safely","validation")
def validation(h):
    for raw in (b"null",b"{bad",b"{} {}",b"[]"):
        error(h.call("notifications","POST",raw=raw,headers={"Authorization":"Bearer "+h.token(),"Content-Type":"application/json"}),400,"INVALID_REQUEST")
    for field,value in (("body_encoding","rot13"),("path","/../admin"),("headers",{"Host":"other"}),("headers",{"Authorization":"not-a-secret"}),("headers",{"X-A":"1","x-a":"2"})):
        key,body=h.body();body["request"][field]=value;error(h.submit(key,body),400,"INVALID_REQUEST")
    key,body=h.body();body["request"]["method"]="DELETE";error(h.submit(key,body),403,"TARGET_NOT_ALLOWED")
    key,body=h.body();body["endpoint"]="http://other";error(h.submit(key,body),400,"INVALID_REQUEST")
    for query in ("limit=0","limit=101","cursor=invalid","status=unknown","batch_id=invalid"):
        error(h.call("notifications?"+query),400,"INVALID_REQUEST")


@case("P2-04","Bounded response digest and sensitive preview/report redaction","response")
def response_bounds(h):
    identifier,_,_=h.accept("secret");task=h.wait(identifier)
    raw=database("SELECT HEX(response_body_preview),HEX(response_body_hash) FROM delivery_attempt WHERE notification_id=UNHEX('"+identifier.replace("-","")+"')")
    preview,digest=raw.split("\t");preview=bytes.fromhex(preview).decode()
    assert len(digest)==64 and h.secret not in preview and "fixture-password" not in preview and "person@example.test" not in preview
    assert h.secret not in json.dumps(task) and task["attempts"][0]["report"]["password"]=="[REDACTED]"
    for size,expected in ((1<<20,"delivered"),((1<<20)+1,"failed")):
        identifier,_,_=h.accept(response_size=size);task=h.wait(identifier,expected)
        raw=database("SELECT COALESCE(HEX(response_body_hash),'NULL') FROM delivery_attempt WHERE notification_id=UNHEX('"+identifier.replace("-","")+"')")
        if expected=="delivered":assert raw.lower()==hashlib.sha256(b"x"*size).hexdigest()
        else:assert raw=="NULL" and task["last_error_code"]=="response_too_large"


@case("P2-05","Redirect and DNS-resolved private destinations cannot bypass policy","security")
def ssrf(h):
    identifier,key,_=h.accept(statuses="302");h.wait(identifier,"failed")
    assert h.records(key)["redirects"]==0
    identifier,key,_=h.accept("ssrf");task=h.wait(identifier,"failed")
    assert task["last_error_code"]=="destination_blocked" and not h.records(key)["requests"]
    original=copy.deepcopy(h.document);doc=copy.deepcopy(original)
    doc["targets"][h.names["header"]]["endpoint"]["allowed_cidrs"]=[];h.config(doc)
    try:
        identifier,key,_=h.accept();task=h.wait(identifier,"failed");assert task["last_error_code"]=="destination_blocked" and not h.records(key)["requests"]
    finally:h.config(original)


@case("P2-06","Malformed supplier JSON and invalid Hook config fail safely","hook")
def hook_errors(h):
    for target,query in (("broken-hook",{}),("hook",{"malformed":"true"})):
        identifier,_,_=h.accept(target,**query);task=h.wait(identifier,"failed")
        assert task["last_error_code"]=="hook_evaluation_failed" and task["attempt_count"]==1
    revision=database("SELECT revision FROM config_revision WHERE scope='global'","control")
    bad=copy.deepcopy(h.document);bad["hooks"][h.names["hook"]]["expression"]="{invalid CEL"
    path=h.directory/"invalid-management.json";path.write_text(json.dumps(bad));path.chmod(0o644)
    result=subprocess.run(COMPOSE+["run","--rm","--no-deps","-v",str(path)+":/invalid.json:ro","config-init","-file","/invalid.json","-apply","-actor",h.run_id],capture_output=True,text=True)
    assert result.returncode!=0 and database("SELECT revision FROM config_revision WHERE scope='global'","control")==revision
    identifier,_,_=h.accept();h.wait(identifier);h.health()


@case("P2-07","Secret atomic rotation and invalid process file preserve LKG","config")
def filewatch(h):
    identifier,key,_=h.accept("secret");h.wait(identifier)
    assert h.records(key)["requests"][0]["auth_hash"]==hashlib.sha256(h.secret.encode()).hexdigest()
    replacement=uuid.uuid4().hex;tmp=h.secret_file.with_suffix(".new");tmp.write_text(replacement);tmp.chmod(0o644);tmp.replace(h.secret_file)
    def rotated():
        identifier,key,_=h.accept("secret");h.wait(identifier)
        return h.records(key)["requests"][0]["auth_hash"]==hashlib.sha256(replacement.encode()).hexdigest()
    eventually(rotated,40,1);h.secret=replacement
    valid=h.process_file.read_bytes();start=datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
        h.process_file.write_text('{"workers":0}')
        def rejected():
            return all('"msg":"config_reload_failed","source":"file"' in compose("logs","--no-color","--no-log-prefix","--since",start,service) for service in ("notifier","notifier-2"))
        eventually(rejected,40)
        identifier,_,_=h.accept();h.wait(identifier);h.health()
    finally:h.process_file.write_bytes(valid)


@case("P2-08","API-only role readiness is independent of unavailable MQ","roles")
def roles(h):
    h.overlay_data["services"]["notifier"]["environment"]={"NOTIFIER_ROLES":"api"}
    h.overlay_up("notifier");h.healthy()
    try:
        compose("stop","rabbitmq")
        eventually(lambda:http("http://127.0.0.1:8083/health/ready")[0]==503)
        assert http("http://127.0.0.1:8081/health/ready")[0]==200
        identifier,_,_=h.accept(instance=0);assert h.task(identifier,0)["status"]=="pending"
    finally:h.start("rabbitmq")
    h.healthy();h.wait(identifier)
    h.overlay_data["services"]["notifier"].pop("environment");h.overlay_up("notifier");h.healthy()


@case("P2-09","Cursor pages and filters retain authenticated client scope","query")
def pagination(h):
    identifiers=[h.accept(instance=i%2)[0] for i in range(5)]
    for identifier in identifiers:h.wait(identifier)
    all_ids=[];cursor=""
    for i in range(200):
        code,page,_=h.call("notifications?limit=2&status=delivered&target="+h.names["header"]+"&cursor="+cursor,instance=i%2)
        assert code==200
        all_ids.extend(item["notification_id"] for item in page["items"])
        cursor=page["next_cursor"]
        if not cursor:break
    assert not cursor and len(all_ids)==len(set(all_ids)) and set(identifiers)<=set(all_ids)
    code,other,_=h.call("notifications?limit=100",client="other")
    assert code==200 and not ({i["notification_id"] for i in other["items"]}&set(all_ids))
    return {"pages":i+1,"unique_items":len(all_ids)}


@case("P2-10","Graceful drain allows active HTTP completion and peer service","lifecycle")
def drain(h):
    identifier,_,_=h.accept(delays_ms="1500")
    def owner():
        for service,port in (("notifier",8081),("notifier-2",8083)):
            if http(f"http://127.0.0.1:{port}/health/detail")[1]["worker_active"]:return service
    service=eventually(owner,10);peer=1 if service=="notifier" else 0
    try:
        compose("stop",service)
        task=h.task(identifier,peer);assert task["status"]=="delivered" and task["attempt_count"]==1
        live,_,_=h.accept(instance=peer)
        eventually(lambda:h.task(live,peer)["status"]=="delivered")
    finally:h.start(service)
    h.healthy();return task


# L9(3^4), every pair of levels appears once for any pair of columns.
L9=[(0,0,0,0),(0,1,1,1),(0,2,2,2),(1,0,1,2),(1,1,2,0),(1,2,0,1),(2,0,2,1),(2,1,0,2),(2,2,1,0)]
# L8(2^4), every pair appears twice; no invalid authentication masks other axes.
L8=[(a,b,c,a^b^c) for a,b,c in itertools.product(range(2),repeat=3)]


def orthogonal9(h,levels):
    response,mode,injection,entry=levels
    target=("header","query","body")[injection]
    key,body=h.body(target,statuses=("200","503,200","400")[response],mode=("async","proxy","async")[mode])
    def send(instance):
        if mode==2:
            code,result,_=h.call("notifications:batch","POST",{"items":[{**body,"idempotency_key":key}]},instance=instance)
            assert code==202 and result["results"][0]["accepted"],result
            identifier=result["results"][0]["notification_id"];h.ids.append(identifier);return identifier
        code,result,_=h.submit(key,body,instance);assert code in (200,202);return result["notification_id"]
    if entry==2:
        with ThreadPoolExecutor(max_workers=2) as pool:identifiers=list(pool.map(send,(0,1)))
    else:identifiers=[send(entry)]
    assert len(set(identifiers))==1
    task=h.wait(identifiers[0],"failed" if response==2 else "delivered")
    assert task["attempt_count"]==(2 if response==1 else 1)
    for row in h.records(key)["requests"]:
        value=row["header_key"] if injection==0 else row["query_key"] if injection==1 else row["body"]["items"][0]["request_id"]
        assert value==key
    return {"levels":levels,"task":task}


def orthogonal8(h,levels):
    mode,method,encoding,instance=levels
    key,body=h.body(mode=("async","proxy")[mode])
    body["request"]["method"]=("POST","PUT")[method]
    expected=hashlib.sha256(body["request"]["body"].encode()).hexdigest()
    if encoding:body["request"]["body_encoding"]="base64";body["request"]["body"]=base64.b64encode(body["request"]["body"].encode()).decode()
    code,value,_=h.submit(key,body,instance);assert code in (200,202)
    task=h.wait(value["notification_id"]);rows=h.records(key)["requests"]
    assert task["attempt_count"]==1 and len(rows)==1 and rows[0]["body_hash"]==expected
    return {"levels":levels,"id":task["notification_id"]}


for index,levels in enumerate(L9,1):
    CASES.append({"id":f"O1-{index:02}","priority":"P1","title":"L9 response/mode/injection/entry "+str(levels),"group":"O1","levels":levels,"run":lambda h,levels=levels:orthogonal9(h,levels)})
for index,levels in enumerate(L8,1):
    CASES.append({"id":f"O2-{index:02}","priority":"P2","title":"L8 mode/method/encoding/instance "+str(levels),"group":"O2","levels":levels,"run":lambda h,levels=levels:orthogonal8(h,levels)})
