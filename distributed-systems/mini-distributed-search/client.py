# client.py
import os
import shlex
import subprocess
import sys
import time
import json
from typing import Optional

import pika

from common import AMQP_URL, MANAGER_RPC_QUEUE

class RPCClient:
    def __init__(self):
        params = pika.URLParameters(AMQP_URL)
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()
        self.ch.queue_declare(queue=MANAGER_RPC_QUEUE, durable=False, exclusive=False, auto_delete=False)

        cbq = self.ch.queue_declare(queue="", exclusive=True)
        self.callback_queue = cbq.method.queue
        self.response = None
        self.corr_id = None
        self.ch.basic_consume(queue=self.callback_queue, on_message_callback=self._on_resp, auto_ack=True)

    def _on_resp(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            try:
                self.response = json.loads(body.decode("utf-8"))
            except Exception:
                self.response = {"ok": False, "error": "invalid-json"}

    def call(self, payload: dict, timeout: float = 5.0) -> dict:
        self.response = None
        self.corr_id = str(time.time_ns())
        try:
            self.ch.basic_publish(
                exchange="",
                routing_key=MANAGER_RPC_QUEUE,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=self.corr_id
                ),
                body=json.dumps(payload).encode("utf-8"),
            )
        except Exception as e:
            return {"ok": False, "error": f"publish-failed: {e}"}
        start = time.time()
        while self.response is None:
            self.conn.process_data_events(time_limit=0.2)
            if time.time() - start > timeout:
                return {"ok": False, "error": "timeout"}
        return self.response

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

def project_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))

def _print_leader_from(resp: dict):
    lid = resp.get("leader")
    if lid is not None:
        print(f"Manager leader: {lid}")

def run():
    print("Client console. Type 'help' for a list of commands.")
    procs = {"managers": [], "workers": []}
    rpc: Optional[RPCClient] = None

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            line = "quit"

        if not line:
            continue

        parts = shlex.split(line)
        cmd = parts[0].lower()

        if cmd in {"quit", "exit"}:
            print("Bye.")
            for p in procs["workers"] + procs["managers"]:
                if p and p.poll() is None:
                    p.terminate()
            if rpc:
                try:
                    rpc.close()
                except Exception:
                    pass
            break

        if cmd == "help":
            print("Commands:")
            print("  start <workers> <k_spares>   — launch k workers with replicas and (k_spares+1) managers")
            print("  load <folder> <e|u>          — load text files even/uneven")
            print("  find <word>                  — search")
            print("  find_like <text>             — search for similar documents using TF-IDF")
            print("  purge                        — clear all data")
            print("  exit                         — quit")
            continue

        if cmd == "start":
            if len(parts) != 3:
                print("Usage: start <workers> <k_spares>")
                continue
            try:
                k = int(parts[1])
                k_spares = int(parts[2])
                if k <= 0 or k_spares < 0:
                    raise ValueError
            except ValueError:
                print("Both <workers> and <k_spares> must be integers (workers>0, spares>=0).")
                continue
            if rpc:
                print("Already started.")
                continue

            py = sys.executable
            total_managers = k_spares + 1

            for mid in range(total_managers):
                cmdline = f"{py} manager.py --id {mid} --m {total_managers} --k {k}"
                p = subprocess.Popen(shlex.split(cmdline), cwd=project_dir())
                procs["managers"].append(p)

            for i in range(k):
                rid = i + k
                p1 = subprocess.Popen(shlex.split(f"{py} worker.py --id {i} --replica-id {rid} --role primary"), cwd=project_dir())
                p2 = subprocess.Popen(shlex.split(f"{py} worker.py --id {rid} --replica-id {i} --role replica"), cwd=project_dir())
                procs["workers"].extend([p1, p2])

            time.sleep(1.0)

            try:
                rpc = RPCClient()
            except Exception as e:
                print(f"Cannot connect: {e}")
                rpc = None
                continue

            got = False
            for _ in range(10):
                resp = rpc.call({"cmd": "ping"}, timeout=0.8)
                if resp.get("ok"):
                    got = True
                    n_alive = int(resp.get("n_alive", 0))
                    if n_alive == k:
                        print("System ready.")
                    else:
                        print(f"Warning: only {n_alive}/{k} workers alive.")
                        print("System ready (degraded).")
                    _print_leader_from(resp)
                    break
                time.sleep(0.2)

            if not got:
                print("Manager not responding. Is RabbitMQ running?")
            continue

        if cmd == "purge":
            if not rpc:
                print("Not started. Use: start <workers> <k_spares>")
                continue
            resp = rpc.call({"cmd": "purge"}, timeout=4.0)
            if not resp.get("ok") and resp.get("error") == "timeout":
                print("No manager")
                continue
            if resp.get("ok"):
                print("Purged.")
                _print_leader_from(resp)
            else:
                err = resp.get("error", "unknown")
                if "timeout" in err:
                    print("No manager")
                else:
                    print(f"Manager error: {err}")
            continue

        if cmd == "load":
            if not rpc:
                print("Not started. Use: start <workers> <k_spares>")
                continue
            if len(parts) != 3:
                print("Usage: load <folder> <e|u>")
                continue
            folder = parts[1]
            mode = parts[2].lower()
            resp = rpc.call({"cmd": "load", "folder": folder, "mode": mode}, timeout=15.0)
            if not resp.get("ok") and resp.get("error") == "timeout":
                print("No manager")
                continue
            if resp.get("ok"):
                print(f"Loaded {resp.get('loaded', 0)} files.")
                _print_leader_from(resp)
            else:
                print(f"Error: {resp.get('error')}")
            continue

        if cmd == "find":
            if not rpc:
                print("Not started. Use: start <workers> <k_spares>")
                continue
            if len(parts) != 2:
                print("Usage: find <word>")
                continue
            word = parts[1]
            resp = rpc.call({"cmd": 'find', "word": word}, timeout=20.0)
            if not resp.get("ok") and resp.get("error") == "timeout":
                print("No manager")
                continue
            if not resp.get("ok"):
                print(f"Error: {resp.get('error')}")
                continue
            results = resp.get("results", [])
            print(f"Found {len(results)} results.")
            for item in results:
                wid = item["worker"]
                fname = item["file"]
                sent = item["sentence"]
                print(f"[node {wid}] {fname}: {sent}")
            _print_leader_from(resp)
            continue

        if cmd == "find_like":
            if not rpc:
                print("Not started. Use: start <workers> <k_spares>")
                continue
            if len(parts) < 2:
                print("Usage: find_like <text>")
                continue
            # Объединяем все аргументы после команды в один текст
            text = " ".join(parts[1:])
            resp = rpc.call({"cmd": "find_like", "text": text}, timeout=20.0)
            if not resp.get("ok") and resp.get("error") == "timeout":
                print("No manager")
                continue
            if not resp.get("ok"):
                print(f"Error: {resp.get('error')}")
                continue
            results = resp.get("results", [])
            print(f"Found {len(results)} similar documents.")
            for i, item in enumerate(results):
                similarity = item["similarity"]
                fname = item["file"]
                worker_id = item["worker"]
                print(f"{i+1}. [node {worker_id}] {fname} (similarity: {similarity:.4f})")
            _print_leader_from(resp)
            continue

        print(f"Unknown command: {cmd}. Type 'help'.")

if __name__ == "__main__":
    run()