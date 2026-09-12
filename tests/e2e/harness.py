"""Runtime environment, fault injection and observation helpers for E2E cases."""
import base64
import copy
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from runtime import COMPOSE, compose, database, rabbit


def eventually(fn, timeout=90, interval=.25):
    end = time.monotonic()+timeout
    last = None
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except (AssertionError, OSError, urllib.error.URLError) as error:
            last = str(error)
        time.sleep(interval)
    raise AssertionError(f"condition timed out: {last}")


def http(url, method="GET", body=None, headers=None, raw=None):
    h = dict(headers or {})
    if raw is None and body is not None:
        raw = json.dumps(body, separators=(",", ":")).encode()
        h.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, method=method, data=raw, headers=h)
    try:
        response = urllib.request.urlopen(req, timeout=45)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        data = response.read()
        try: value = json.loads(data) if data else None
        except json.JSONDecodeError: value = data.decode(errors="replace")
        return response.status, value, dict(response.headers)


def sqlquote(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


class Harness:
    def __init__(self):
        os.chdir(ROOT)
        self.run_id = "e2e-"+uuid.uuid4().hex[:10]
        self.directory = ROOT/".runtime"/"e2e"/self.run_id
        self.directory.mkdir(parents=True, mode=0o700)
        self.lock = (ROOT/".runtime"/"e2e.lock").open("w")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.original = json.loads((ROOT/"configs/secrets/management.json").read_text())
        self.process_file = ROOT/"configs/secrets/notifier.json"
        self.original_process = self.process_file.read_bytes()
        self.document = copy.deepcopy(self.original)
        self.names = {}
        self.ids = []
        self.owners = {}
        self.known_cases = {}
        self.case_name = "setup"
        self.seq = 0
        self.provider_name = self.run_id+"-provider"
        self.overlay = self.directory/"compose.override.json"
        self.secret = secrets.token_hex(24)
        self.secret_dir = self.directory/"secrets"
        self.secret_dir.mkdir(mode=0o755)
        self.secret_file = self.secret_dir/"current"
        self.secret_file.write_text(self.secret)
        self.secret_file.chmod(0o644)
        self.overlay_data = {"services": {service: {"volumes": [str(self.secret_dir)+":/e2e-secrets:ro"]} for service in ("notifier", "notifier-2")}}
        self.results = []

    def save(self, name, value):
        (self.directory/name).write_text(json.dumps(value, indent=2, default=str)+"\n")

    def token(self, client="client", changes=None, algorithm="RS256"):
        def enc(v): return base64.urlsafe_b64encode(json.dumps(v, separators=(",", ":")).encode()).decode().rstrip("=")
        claims = {"iss":"internal-auth", "sub":"e2e-service", "aud":"notification-service", "exp":int(time.time())+3600, "client_id":self.names.get(client, client)}
        for k,v in (changes or {}).items():
            if v is None: claims.pop(k,None)
            else: claims[k]=v
        text = enc({"alg":algorithm, "kid":"local", "typ":"JWT"})+"."+enc(claims)
        signature = subprocess.run(["openssl", "dgst", "-sha256", "-sign", str(ROOT/"configs/secrets/jwt-private.pem")], input=text.encode(), check=True, capture_output=True).stdout
        return text+"."+base64.urlsafe_b64encode(signature).decode().rstrip("=")

    def call(self, path, method="GET", body=None, instance=0, client="client", headers=None, raw=None):
        h = {"Authorization":"Bearer "+self.token(client)} if headers is None else headers
        return http(f"http://127.0.0.1:{8080+2*instance}/api/v1/"+path, method, body, h, raw)

    def health(self, ready=True):
        values=[]
        for port in (8081,8083):
            status, data, _ = http(f"http://127.0.0.1:{port}/health/detail")
            assert status==200 and data["ready"] == ready, data
            values.append(data)
        assert values[0]["instance_id"] != values[1]["instance_id"]
        return values

    def healthy(self):
        return eventually(lambda:self.health(), timeout=90)

    def body(self, target="header", statuses="200", mode="async", **query):
        self.seq+=1
        key=f"{self.run_id}-{self.case_name}-{self.seq}"
        return key, {"target":self.names[target], "delivery_mode":mode, "request":{"method":"POST", "path":"/deliver", "query":{"case":key,"statuses":statuses,**{k:str(v) for k,v in query.items()}}, "headers":{"Content-Type":"application/json"}, "body":"{\"order_id\":\"order-e2e\",\"items\":[{}]}"}}

    def submit(self, key, body, instance=0, client="client"):
        code, value, headers=self.call("notifications","POST",body,instance,headers={"Authorization":"Bearer "+self.token(client),"Idempotency-Key":key})
        if code in (200,202):
            identifier=value["notification_id"]
            if identifier not in self.ids:self.ids.append(identifier)
            self.owners[identifier]=client
            self.known_cases[identifier]=key
        return code,value,headers

    def accept(self, target="header", statuses="200", mode="async", instance=0, **query):
        key,body=self.body(target,statuses,mode,**query)
        code,value,_=self.submit(key,body,instance)
        assert code in (200,202),(code,value)
        return value["notification_id"], key, body

    def task(self, identifier, instance=1, client=None):
        client=client or self.owners.get(identifier,"client")
        code,value,_=self.call("notifications/"+identifier,instance=instance,client=client)
        assert code==200,(code,value)
        return value

    def wait(self, identifier, expected="delivered", timeout=100):
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            data=self.task(identifier)
            if data["status"]==expected:return data
            assert data["status"] not in ("delivered","failed"),data
            time.sleep(.2)
        raise AssertionError(("task timeout",identifier,data))

    def records(self,key):
        code,data,_=http("http://127.0.0.1:18091/observations?case="+urllib.parse.quote(key))
        assert code==200
        return data

    def config(self, document=None, wait=True):
        next_doc=copy.deepcopy(document if document is not None else self.document)
        # Entity revisions increase only when their content changes. The admin
        # command still performs independent validation before committing.
        current_revision=int(database("SELECT revision FROM config_revision WHERE scope='global'","control"))
        previous=json.loads(bytes.fromhex(database(f"SELECT HEX(document) FROM config_snapshot WHERE revision={current_revision}","control")))
        for kind in ("targets","retry_policies","hooks"):
            for name,value in next_doc[kind].items():
                old=previous[kind].get(name)
                if old:
                    a={k:v for k,v in old.items() if k!="revision"};b={k:v for k,v in value.items() if k!="revision"}
                    value["revision"]=max(value.get("revision",1),old["revision"]+(a!=b))
        path=self.directory/"management.json";path.write_text(json.dumps(next_doc));path.chmod(0o644)
        compose("run","--rm","--no-deps","-v",str(path)+":/e2e-management.json:ro","config-init","-file","/e2e-management.json","-apply","-actor",self.run_id)
        self.document=next_doc
        rev=int(database("SELECT revision FROM config_revision WHERE scope='global'","control"))
        if wait:eventually(lambda: all(d["config_revision"]==rev for d in self.health()),timeout=35)
        return rev

    def overlay_up(self, *services):
        self.overlay.write_text(json.dumps(self.overlay_data))
        return subprocess.run(COMPOSE+["-f",str(self.overlay),"up","-d","--no-deps","--wait","--wait-timeout","120",*services],check=True,capture_output=True,text=True)

    def setup(self):
        self.healthy()
        binary=self.directory/"provider"
        subprocess.run(["go","build","-o",str(binary),"./tests/e2e/provider"],env={**os.environ,"CGO_ENABLED":"0"},check=True)
        subprocess.run(["docker","run","-d","--name",self.provider_name,"--network","notifier_default","--network-alias","e2e-provider","--read-only","--cap-drop","ALL","-p","127.0.0.1:18091:8080","-v",str(binary)+":/e2e-provider:ro","--entrypoint","/e2e-provider","notifier:local"],check=True,capture_output=True)
        eventually(lambda:http("http://127.0.0.1:18091/health/live")[0]==200)
        p=json.loads(self.original_process);p.setdefault("secrets",{})[self.run_id]={"file":"/e2e-secrets/current"}
        self.process_file.write_text(json.dumps(p));self.overlay_up("notifier","notifier-2")
        self.healthy()
        for name in ("client","other","limited","header","query","body","hook","report","broken-hook","short","deadline","no-retry","unknown","uncertain","rate","serial","closed","secret","ssrf"):
            self.names[name]=self.run_id+"-"+name
        d=self.document
        q=copy.deepcopy(d["quota_policies"]["default"]);q.update(ingress_client=1000,egress_target=1000,egress_client_target=1000,target_concurrency=32)
        d["quota_policies"][self.run_id]=q
        for name,patch in (("rate",{"egress_target":1}),("serial",{"target_concurrency":1}),("limited",{"ingress_client":1}),("closed",{"fail_mode":"open"})):
            d["quota_policies"][self.names[name]]={**q,**patch}
        d["retry_policies"][self.run_id]={"revision":1,"delays":["5s"],"max_duration":"2m","respect_retry_after":True}
        d["retry_policies"][self.names["deadline"]]={"revision":1,"delays":["5s"],"max_duration":"3s","respect_retry_after":True}
        d["hooks"][self.names["hook"]]={"revision":1,"expression":"{'action': json.code == 0 ? 'success' : (json.code == 1001 ? 'retry' : 'fail'), 'reason': 'business_code', 'report': {'code':json.code}}"}
        d["hooks"][self.names["report"]]={"revision":1,"expression":"{'action':'report','report':json}"}
        d["hooks"][self.names["broken-hook"]]={"revision":1,"expression":"{'action':json.missing_field}"}
        template=copy.deepcopy(d["targets"]["target-a"])
        template.update(revision=1,hook="",retry_policy=self.run_id,quota_policy=self.run_id,allow_proxy=True)
        template["endpoint"].update(base_url="http://e2e-provider:8080",allowed_hosts=["e2e-provider"],allowed_methods=["POST","PUT","PATCH"],allowed_path_prefixes=["/deliver"],allowed_cidrs=["172.29.0.0/24"])
        for name in ("header","query","body","hook","report","broken-hook","short","deadline","no-retry","unknown","uncertain","rate","serial","closed","secret","ssrf"):
            t=copy.deepcopy(template)
            if name=="query":t["idempotency"]["inject"]=[{"location":"query","key":"partner_id"}]
            if name=="body":t["idempotency"]["inject"]=[{"location":"body_json","json_pointer":"/items/0/request_id"}]
            if name in ("hook","report","broken-hook"):t["hook"]=self.names[name]
            if name=="deadline":t["retry_policy"]=self.names[name]
            if name=="no-retry":t["retry_enabled"]=False
            if name in ("unknown","uncertain"):
                t["idempotency"]={"mode":"unknown","allow_uncertain_retry":name=="uncertain"}
            if name in ("rate","serial"):t["quota_policy"]=self.names[name]
            # Closed is activated only inside its fault case, so other fault
            # groups retain fail-open readiness semantics.
            if name=="secret":t["auth"]={"type":"static_header","header":"Authorization","secret_ref":self.run_id};t["hook"]=self.names["report"]
            if name=="ssrf":t["endpoint"].update(base_url="http://127.0.0.1:8080",allowed_hosts=["127.0.0.1"],allowed_cidrs=[])
            d["targets"][self.names[name]]=t
        targets=[v for k,v in self.names.items() if k not in ("client","other","limited")]
        for client in ("client","other","limited"):
            d["clients"][self.names[client]]={"enabled":True,"targets":targets,"manual_retry":client=="client","quota_policy":self.names["limited"] if client=="limited" else self.run_id}
        self.config()

    def capture(self):
        for service in ("notifier","notifier-2"):
            (self.directory/(service+".log")).write_text(compose("logs","--no-color","--no-log-prefix",service))
        self.save("health.json",self.health())
        self.save("case-tasks.json",{i:self.task(i) for i in self.ids if self.task(i)["status"] in ("failed","delivered")})

    def cleanup(self):
        # Restart only this project's dependencies. docker start avoids rerunning
        # Compose's config-init job with a different active document mid-suite.
        for service in ("mariadb","redis","rabbitmq","notifier","notifier-2"):
            ids=compose("ps","--all","--quiet",service).split()
            for identifier in ids:subprocess.run(["docker","start",identifier],check=True,capture_output=True)
        self.healthy()
        # Restore active content with a NEW revision; retained task history must
        # never be rewritten or deleted merely to make a case pass.
        self.config(self.original)
        self.process_file.write_bytes(self.original_process)
        subprocess.run(COMPOSE+["up","-d","--no-deps","--wait","--wait-timeout","120","notifier","notifier-2"],check=True,capture_output=True)
        subprocess.run(["docker","rm","-f",self.provider_name],check=True,capture_output=True)
        self.healthy()

    def start(self, service):
        for identifier in compose("ps","--all","--quiet",service).split():
            subprocess.run(["docker","start",identifier],check=True,capture_output=True)

    def publish(self, payload):
        # RabbitMQ management is only a transport fault injector, never an
        # alternative implementation of the Notification state machine.
        env=dict(line.split("=",1) for line in (ROOT/".env").read_text().splitlines() if line and not line.startswith("#"))
        auth=base64.b64encode(("notifier:"+env["AMQP_PASSWORD"]).encode()).decode()
        code,value,_=http("http://127.0.0.1:15672/api/exchanges/%2F/notify.dispatch.x/publish","POST",{"properties":{"delivery_mode":2},"routing_key":"dispatch","payload":payload,"payload_encoding":"string"},{"Authorization":"Basic "+auth})
        assert code==200 and value["routed"],value

    def queue_get(self, queue):
        env=dict(line.split("=",1) for line in (ROOT/".env").read_text().splitlines() if line and not line.startswith("#"))
        auth=base64.b64encode(("notifier:"+env["AMQP_PASSWORD"]).encode()).decode()
        code,data,_=http("http://127.0.0.1:15672/api/queues/%2F/"+queue+"/get","POST",{"count":1,"ackmode":"ack_requeue_false","encoding":"auto","truncate":4096},{"Authorization":"Basic "+auth})
        assert code==200,data
        return data
