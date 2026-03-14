# manager.py
import argparse
import json
import os
import time
import random
import math
from typing import Dict, List, Tuple, Optional, Any

import pika

from common import (
    AMQP_URL, MANAGER_RPC_QUEUE, WORKER_QUEUE_FMT,
    MANAGER_CTRL_QUEUE_FMT, MANAGER_BROADCAST_EXCHANGE,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
    ELECTION_REPLY_TIMEOUT, COORDINATOR_WAIT_TIMEOUT,
    extract_words, calculate_cosine_similarity
)

def _list_text_files(folder: str) -> List[str]:
    paths: List[str] = []
    for root, _, files in os.walk(folder):
        for fn in files:
            if fn.lower().endswith(".txt"):
                paths.append(os.path.join(root, fn))
    paths.sort()
    return paths

class WorkerRPC:
    def __init__(self):
        params = pika.URLParameters(AMQP_URL)
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()

        cbq = self.ch.queue_declare(queue="", exclusive=True)
        self.callback_queue = cbq.method.queue
        self._responses: Dict[str, dict] = {}
        self.ch.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_worker_resp,
            auto_ack=True,
        )

    def _on_worker_resp(self, ch, method, props, body):
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception:
            data = {"ok": False, "error": "invalid-json"}
        self._responses[props.correlation_id] = data

    def call(self, worker_id: int, payload: dict, timeout: float = 4.0) -> dict:
        corr_id = str(time.time_ns())
        qname = WORKER_QUEUE_FMT.format(id=int(worker_id))
        self.ch.queue_declare(queue=qname, durable=False, exclusive=False, auto_delete=False)
        self._responses.pop(corr_id, None)
        self.ch.basic_publish(
            exchange="",
            routing_key=qname,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=corr_id,
            ),
            body=json.dumps(payload).encode("utf-8"),
        )
        start = time.time()
        while corr_id not in self._responses:
            self.conn.process_data_events(time_limit=0.2)
            if time.time() - start > timeout:
                return {"ok": False, "error": "timeout"}
        return self._responses.pop(corr_id)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

class Manager:
    def __init__(self, manager_id: int, n_managers: int, n_prim_workers: int):
        self.id = int(manager_id)
        self.n_managers = int(n_managers)
        self.k = int(n_prim_workers)    
        self.n_prim = self.k

        self.leader_id: Optional[int] = None
        self.is_leader = False
        self.last_heartbeat: float = 0.0

        self._election_in_progress = False
        self._election_ok_received = False
        self._election_deadline = 0.0
        self._coord_deadline = 0.0

        self.file_to_worker: Dict[str, int] = {}
        
        # Структуры для TF-IDF
        self.tf_idf_vectors: Dict[str, Dict[str, float]] = {}  # doc_id -> {word: tf-idf}
        self.document_frequencies: Dict[str, int] = {}  # word -> df
        self.total_documents: int = 0
        self.document_lengths: Dict[str, int] = {}  # doc_id -> общее количество слов

        params = pika.URLParameters(AMQP_URL)
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()

        for wid in range(self.k):
            q = WORKER_QUEUE_FMT.format(id=wid)
            self.ch.queue_declare(queue=q, durable=False, exclusive=False, auto_delete=False)
            rq = WORKER_QUEUE_FMT.format(id=wid + self.k)
            self.ch.queue_declare(queue=rq, durable=False, exclusive=False, auto_delete=False)

        self.ctrl_queue = MANAGER_CTRL_QUEUE_FMT.format(id=self.id)
        self.ch.queue_declare(queue=self.ctrl_queue, durable=False, exclusive=False, auto_delete=True)

        self.ch.exchange_declare(exchange=MANAGER_BROADCAST_EXCHANGE, exchange_type="fanout", durable=False)
        self.ch.queue_bind(queue=self.ctrl_queue, exchange=MANAGER_BROADCAST_EXCHANGE)

        self.ch.basic_consume(queue=self.ctrl_queue, on_message_callback=self.on_ctrl, auto_ack=True)

        self._leader_consumer_tag: Optional[str] = None
        self.worker_rpc = WorkerRPC()

    def _become_leader(self):
        if self.is_leader:
            return
        self.is_leader = True
        self.leader_id = self.id
        if self._leader_consumer_tag is None:
            self.ch.queue_declare(queue=MANAGER_RPC_QUEUE, durable=False, exclusive=False, auto_delete=False)
            self._leader_consumer_tag = self.ch.basic_consume(
                queue=MANAGER_RPC_QUEUE, on_message_callback=self.on_client_request, auto_ack=False
            )
        self._broadcast({"type": "COORDINATOR", "id": self.id})
        self._send_heartbeat()

    def _step_down(self):
        if not self.is_leader:
            return
        self.is_leader = False
        if self._leader_consumer_tag is not None:
            try:
                self.ch.basic_cancel(self._leader_consumer_tag)
            except Exception:
                pass
            self._leader_consumer_tag = None

    def _send(self, to_id: int, msg: dict):
        try:
            q = MANAGER_CTRL_QUEUE_FMT.format(id=int(to_id))
            self.ch.queue_declare(queue=q, durable=False, exclusive=False, auto_delete=True)
            self.ch.basic_publish(exchange="", routing_key=q, body=json.dumps(msg).encode("utf-8"))
        except Exception:
            pass

    def _broadcast(self, msg: dict):
        try:
            self.ch.basic_publish(exchange=MANAGER_BROADCAST_EXCHANGE, routing_key="", body=json.dumps(msg).encode("utf-8"))
        except Exception:
            pass

    def _send_heartbeat(self):
        self._broadcast({"type": "HEARTBEAT", "id": self.id, "ts": time.time()})

    def _start_election(self):
        self._election_in_progress = True
        self._election_ok_received = False
        self._election_deadline = time.time() + ELECTION_REPLY_TIMEOUT
        self._coord_deadline = 0.0
        self._step_down()
        higher = [i for i in range(self.n_managers) if i > self.id]
        if not higher:
            pass
        for hid in higher:
            self._send(hid, {"type": "ELECTION", "from": self.id})

    def on_ctrl(self, ch, method, props, body):
        try:
            msg = json.loads(body.decode("utf-8"))
        except Exception:
            return
        mtype = msg.get("type")

        if mtype == "HEARTBEAT":
            lid = int(msg.get("id"))
            self.leader_id = lid
            self.last_heartbeat = time.time()
            if lid != self.id:
                self._step_down()
            return
        if mtype == "ELECTION":
            frm = int(msg.get("from"))
            if self.id > frm:
                self._send(frm, {"type": "OK", "from": self.id})
                if not self._election_in_progress:
                    self._start_election()
            else:
                pass
            return
        if mtype == "OK":
            self._election_ok_received = True
            self._coord_deadline = time.time() + COORDINATOR_WAIT_TIMEOUT
            return
        if mtype == "COORDINATOR":
            lid = int(msg.get("id"))
            self.leader_id = lid
            self.last_heartbeat = time.time()
            self._election_in_progress = False
            if lid == self.id:
                self._become_leader()
            else:
                self._step_down()
            return

    def _reply(self, props, msg: dict):
        try:
            self.ch.basic_publish(
                exchange="",
                routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
                body=json.dumps(msg).encode("utf-8"),
            )
        except Exception:
            pass

    def _assign_even(self, files: List[str]) -> Dict[int, List[str]]:
        mapping: Dict[int, List[str]] = {i: [] for i in range(self.k)}
        if not files or self.k <= 0:
            return mapping
        for i, f in enumerate(files):
            mapping[i % self.k].append(f)
        return mapping

    def _calculate_tf_idf_for_query(self, query_text: str) -> Dict[str, float]:
        """Вычислить TF-IDF вектор для запроса"""
        words = extract_words(query_text, remove_stopwords=True)
        
        if not words:
            return {}
        
        # Вычислить частоту слов в запросе
        word_freq = {}
        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1
        
        # Вычислить TF для запроса
        query_vector = {}
        total_words = len(words)
        
        for word, freq in word_freq.items():
            tf = freq / total_words
            
            # Получить DF из глобальных данных
            df = self.document_frequencies.get(word, 0)
            if df > 0 and self.total_documents > 0:
                idf = math.log10(self.total_documents / df)
            else:
                idf = 0
            
            query_vector[word] = tf * idf
        
        return query_vector

    async def _run_mapreduce(self):
        """Выполнить MapReduce для вычисления TF-IDF"""
        print(f"[manager {self.id}] Running TF-IDF MapReduce...")
        
        # Этап 1: подсчет вхождений каждого слова в каждый документ
        stage1_results = []
        for wid in range(self.k):
            r = self._call_main_or_replica(wid, {"cmd": "mapreduce_stage1"}, timeout=5.0)
            if r.get("ok"):
                stage1_results.extend(r.get("results", []))
        
        # Этап 2: подсчет количества слов в каждом документе
        stage2_results = []
        for wid in range(self.k):
            r = self._call_main_or_replica(wid, {
                "cmd": "mapreduce_stage2",
                "stage1_results": stage1_results
            }, timeout=5.0)
            if r.get("ok"):
                stage2_results.extend(r.get("results", []))
        
        # Обновить общее количество документов
        self.total_documents = len(self.file_to_worker)
        
        # Этап 3: вычисление TF-IDF
        tf_idf_results = []
        for wid in range(self.k):
            r = self._call_main_or_replica(wid, {
                "cmd": "mapreduce_stage3",
                "stage2_results": stage2_results,
                "total_docs": self.total_documents
            }, timeout=5.0)
            if r.get("ok"):
                tf_idf_results.extend(r.get("results", []))
        
        # Обновить структуры данных
        self.tf_idf_vectors.clear()
        self.document_frequencies.clear()
        self.document_lengths.clear()
        
        # Обработать результаты TF-IDF
        for item in tf_idf_results:
            (word, doc_id) = item["key"]
            tf_idf = item["value"]
            
            if doc_id not in self.tf_idf_vectors:
                self.tf_idf_vectors[doc_id] = {}
            
            self.tf_idf_vectors[doc_id][word] = tf_idf
        
        # Вычислить document frequencies
        for doc_id, vector in self.tf_idf_vectors.items():
            for word in vector.keys():
                self.document_frequencies[word] = self.document_frequencies.get(word, 0) + 1
        
        # Вычислить длины документов
        for doc_id, vector in self.tf_idf_vectors.items():
            self.document_lengths[doc_id] = len(vector)
        
        print(f"[manager {self.id}] TF-IDF MapReduce completed. Total docs: {self.total_documents}")

    def on_client_request(self, ch, method, props, body):
        try:
            req = json.loads(body.decode("utf-8"))
        except Exception as e:
            self._reply(props, {"ok": False, "error": f"bad-request: {e}", "leader": self.leader_id})
            ch.basic_ack(method.delivery_tag)
            return
        
        cmd = (req.get("cmd") or "").strip().lower()
        try:
            if cmd == "leader":
                self._reply(props, {"ok": True, "leader": self.leader_id})
            elif cmd == "ping":
                alive = 0
                for wid in range(self.k):
                    r = self._call_main_or_replica(wid, {"cmd": "ping"}, timeout=1.0)
                    if r.get("ok"):
                        alive += 1
                self._reply(props, {"ok": True, "n_alive": alive, "k": self.k, "leader": self.leader_id})
            elif cmd == "purge":
                ok = 0
                for wid in range(self.k):
                    r = self._call_main_or_replica(wid, {"cmd": "purge"}, timeout=2.0)
                    if r.get("ok"):
                        ok += 1
                self.file_to_worker.clear()
                self.tf_idf_vectors.clear()
                self.document_frequencies.clear()
                self.total_documents = 0
                self.document_lengths.clear()
                self._reply(props, {"ok": True, "cleared": ok, "leader": self.leader_id})
            elif cmd == "load":
                folder = (req.get("folder") or "").strip()
                mode = (req.get("mode") or "e").strip().lower()
                if not folder or mode not in {"e", "u"}:
                    self._reply(props, {"ok": False, "error": "usage: load <folder> <e|u>", "leader": self.leader_id})
                else:
                    files = _list_text_files(folder)
                    if mode == "e":
                        mapping = self._assign_even(files)
                    else:
                        mapping: Dict[int, List[str]] = {i: [] for i in range(self.k)}
                        if self.k == 1:
                            mapping[0] = list(files)
                        else:
                            last = self.k - 1
                            first = min(self.k - 1, len(files))
                            for wid in range(first):
                                mapping[wid].append(files[wid])
                            for fp in files[first:]:
                                mapping[last].append(fp)

                    total = 0
                    file_to_worker: Dict[str, int] = {}
                    for wid, lst in mapping.items():
                        if not lst:
                            continue
                        r = self._call_main_or_replica(wid, {"cmd": "load", "files": lst}, timeout=6.0)
                        if r.get("ok"):
                            loaded = int(r.get("loaded", 0))
                            total += loaded
                            for f in lst[:loaded]:
                                file_to_worker[os.path.abspath(f)] = wid
                    self.file_to_worker.update(file_to_worker)
                    
                    # Запустить MapReduce для вычисления TF-IDF
                    import asyncio
                    asyncio.run(self._run_mapreduce())
                    
                    self._reply(props, {"ok": True, "loaded": total, "leader": self.leader_id})
            elif cmd == "find":
                word = (req.get("word") or "").strip()
                if not word:
                    self._reply(props, {"ok": False, "error": "usage: find <word>", "leader": self.leader_id})
                else:
                    results = []
                    for wid in range(self.k):
                        r = self._call_main_or_replica(wid, {"cmd": "find", "word": word}, timeout=4.0)
                        if r.get("ok"):
                            wid_from = int(r.get("id", wid))
                            for item in r.get("results", []):
                                results.append({"worker": wid_from, "file": item["file"], "sentence": item["sentence"]})
                    self._reply(props, {"ok": True, "results": results, "leader": self.leader_id})
            elif cmd == "find_like":
                query_text = (req.get("text") or "").strip()
                if not query_text:
                    self._reply(props, {"ok": False, "error": "usage: find_like <text>", "leader": self.leader_id})
                else:
                    # Вычислить TF-IDF вектор для запроса
                    query_vector = self._calculate_tf_idf_for_query(query_text)
                    
                    if not query_vector:
                        self._reply(props, {"ok": True, "results": [], "leader": self.leader_id})
                        return
                    
                    # Найти похожие документы
                    similarities = []
                    for doc_id, doc_vector in self.tf_idf_vectors.items():
                        similarity = calculate_cosine_similarity(query_vector, doc_vector)
                        similarities.append({
                            "file": doc_id,
                            "similarity": similarity
                        })
                    
                    # Отсортировать по убыванию сходства
                    similarities.sort(key=lambda x: x["similarity"], reverse=True)
                    
                    # Взять топ-10 результатов
                    top_results = similarities[:10]
                    
                    # Преобразовать результаты в нужный формат
                    results = []
                    for item in top_results:
                        if item["similarity"] > 0:
                            # Найти воркера для этого документа
                            worker_id = self.file_to_worker.get(item["file"], -1)
                            results.append({
                                "file": os.path.basename(item["file"]),
                                "full_path": item["file"],
                                "similarity": round(item["similarity"], 4),
                                "worker": worker_id
                            })
                    
                    self._reply(props, {"ok": True, "results": results, "leader": self.leader_id})
            else:
                self._reply(props, {"ok": False, "error": f"unknown-cmd: {cmd}", "leader": self.leader_id})
        except Exception as e:
            self._reply(props, {"ok": False, "error": f"manager-error: {e}", "leader": self.leader_id})
        finally:
            ch.basic_ack(method.delivery_tag)

    def _call_main_or_replica(self, wid: int, payload: dict, timeout: float = 3.0) -> dict:
        try:
            res = self._safe_worker_call(wid, payload, timeout)
        except Exception as e:
            return {"ok": False, "error": f"timeout"}
        
        err = (res or {}).get("error", "")
        if (not res) or (res.get("ok") is False and "timeout" in err):
            rid = wid + self.n_prim
            return self._safe_worker_call(rid, payload, timeout)
        return res

    def _safe_worker_call(self, wid: int, payload: dict, timeout: float):
        try:
            return self.worker_rpc.call(wid, payload, timeout=timeout)
        except Exception:
            try:
                self.worker_rpc.close()
            except Exception:
                pass
            self.worker_rpc = WorkerRPC()
            return self.worker_rpc.call(wid, payload, timeout=timeout)

    def run(self):
        print(f"[manager {self.id}] started (managers={self.n_managers}, primaries={self.k}). AMQP={AMQP_URL}")
        time.sleep(random.uniform(0.1, 0.5))
        self._start_election()

        try:
            while True:
                self.conn.process_data_events(time_limit=0.2)
                now = time.time()

                if self.is_leader and (now - self.last_heartbeat >= HEARTBEAT_INTERVAL):
                    self._send_heartbeat()
                    self.last_heartbeat = now

                if not self.is_leader and (now - self.last_heartbeat > HEARTBEAT_TIMEOUT):
                    if not self._election_in_progress:
                        self._start_election()

                if self._election_in_progress:
                    if not self._election_ok_received and now >= self._election_deadline:
                        self._election_in_progress = False
                        self._become_leader()
                    elif self._election_ok_received and self._coord_deadline and now >= self._coord_deadline:
                        self._start_election()
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self.worker_rpc.close()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass

def main():
    ap = argparse.ArgumentParser("Federated manager with Bully election")
    ap.add_argument("--id", type=int, required=True, help="manager id (0..M-1)")
    ap.add_argument("--m", type=int, required=True, help="total managers (leader + spares)")
    ap.add_argument("--k", type=int, required=True, help="number of primary workers")
    args = ap.parse_args()
    m = Manager(args.id, args.m, args.k)
    m.run()

if __name__ == "__main__":
    main()