# worker.py
import argparse
import json
import os
import math
from typing import Dict, List, Tuple, Any
import hashlib

import pika

from common import (
    AMQP_URL, WORKER_QUEUE_FMT, node_dir, preprocess_line, 
    split_sentences, contains_word, extract_words
)

class Worker:
    def __init__(self, worker_id: int, replica_id: int, role: str):
        self.worker_id = int(worker_id)
        self.replica_id = int(replica_id)
        self.role = role  
        self.store: Dict[str, List[str]] = {}  # {"абс.путь_файла": [список_предложений]}
        self.word_freqs: Dict[str, Dict[str, int]] = {}  # {"абс.путь_файла": {"слово": частота}}
        
        params = pika.URLParameters(AMQP_URL)
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()

        self.queue = WORKER_QUEUE_FMT.format(id=self.worker_id)
        self.ch.queue_declare(queue=self.queue, durable=False, exclusive=False, auto_delete=False)
        self.ch.basic_qos(prefetch_count=1)
        self.ch.basic_consume(queue=self.queue, on_message_callback=self.on_request, auto_ack=False)

        self.ch.queue_declare(queue=WORKER_QUEUE_FMT.format(id=self.replica_id), durable=False, exclusive=False, auto_delete=False)
        os.makedirs(node_dir(self.worker_id), exist_ok=True)

    def _reply(self, props, payload: dict):
        reply_to = getattr(props, "reply_to", None)
        if isinstance(reply_to, bytes):
            reply_to = reply_to.decode("utf-8", errors="ignore")
        if not reply_to:
            return
        corr = getattr(props, "correlation_id", None)
        self.ch.basic_publish(
            exchange="",
            routing_key=reply_to,
            properties=pika.BasicProperties(correlation_id=corr),
            body=json.dumps(payload).encode("utf-8"),
        )

    def _calculate_word_frequencies(self, text: str) -> Dict[str, int]:
        """Вычислить частоту слов в тексте (без стоп-слов)"""
        words = extract_words(text, remove_stopwords=True)
        freq = {}
        for word in words:
            freq[word] = freq.get(word, 0) + 1
        return freq

    def on_request(self, ch, method, props, body):
        try:
            req = json.loads(body.decode("utf-8"))
        except Exception as e:
            self._reply(props, {"ok": False, "error": f"bad-request: {e}"})
            ch.basic_ack(method.delivery_tag)
            return

        cmd = (req.get("cmd") or "").strip().lower()
        try:
            if cmd == "ping":
                self._reply(props, {"ok": True, "id": self.worker_id})
            elif cmd == "purge":
                self.store.clear()
                self.word_freqs.clear()
                self._reply(props, {"ok": True})
            elif cmd == "load":
                files = list(req.get("files") or [])
                loaded = 0
                for fp in files:
                    try:
                        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                            raw = f.read()
                        processed_lines = [preprocess_line(line) for line in raw.splitlines()]
                        processed_text = "\n".join(processed_lines)
                        sents = split_sentences(processed_text)
                        abs_fp = os.path.abspath(fp)
                        
                        # Вычислить частоту слов
                        word_freq = self._calculate_word_frequencies(processed_text)
                        
                        self._send_to_replica({
                            "cmd": "mirror_put",
                            "file": abs_fp,
                            "sentences": sents,
                            "word_freq": word_freq
                        })
                        
                        self.store[abs_fp] = sents
                        self.word_freqs[abs_fp] = word_freq

                        h = hashlib.sha1(abs_fp.encode("utf-8")).hexdigest()[:12]
                        safe_name = f"{h}__{os.path.basename(abs_fp)}.processed.txt"
                        outp = os.path.join(node_dir(self.worker_id), safe_name)
                        with open(outp, "w", encoding="utf-8") as g:
                            g.write(processed_text)
                        loaded += 1
                    except Exception:
                        continue
                self._reply(props, {"ok": True, "loaded": loaded})
            elif cmd == "find":
                word = (req.get("word") or "").strip()
                if not word:
                    self._reply(props, {"ok": False, "error": "usage: find <word>"})
                else:
                    results = []
                    for fp, sents in self.store.items():
                        for s in sents:
                            if contains_word(s, word):
                                results.append({"file": fp, "sentence": s})
                    self._reply(props, {"ok": True, "id": self.worker_id, "results": results})
            elif cmd == "mirror_put":
                fp = req.get("file")
                sents = list(req.get("sentences") or [])
                word_freq = dict(req.get("word_freq") or {})
                if fp:
                    abs_fp = os.path.abspath(fp)
                    self.store[abs_fp] = list(sents)
                    self.word_freqs[abs_fp] = word_freq
                    base = os.path.basename(abs_fp)
                    out = os.path.join(node_dir(self.worker_id), base)
                    with open(out, "w", encoding="utf-8") as f:
                        for s in sents:
                            f.write(s + "\n")
                self._reply(props, {"ok": True})
            elif cmd == "mapreduce_stage1":
                # MapReduce этап 1: подсчет вхождений каждого слова в каждый документ
                results = []
                for doc_id, word_freq in self.word_freqs.items():
                    for word, count in word_freq.items():
                        results.append({
                            "key": (word, doc_id),
                            "value": count
                        })
                self._reply(props, {"ok": True, "results": results})
            elif cmd == "mapreduce_stage2":
                # MapReduce этап 2: преобразование для подсчета слов в документах
                stage1_results = list(req.get("stage1_results") or [])
                results = []
                for item in stage1_results:
                    if item["key"] in self.word_freqs:
                        (word, doc_id) = item["key"]
                        wordcount = item["value"]
                        # Преобразуем в [doc_id, (word, wordcount)]
                        results.append({
                            "key": doc_id,
                            "value": (word, wordcount)
                        })
                self._reply(props, {"ok": True, "results": results})
            elif cmd == "mapreduce_stage3":
                # MapReduce этап 3: вычисление TF-IDF
                stage2_results = list(req.get("stage2_results") or [])
                total_docs = int(req.get("total_docs", 1))
                
                # Сначала соберем DF (сколько документов содержит каждое слово)
                df_counts = {}
                for item in stage2_results:
                    doc_id = item["key"]
                    (word, _) = item["value"]
                    if word not in df_counts:
                        df_counts[word] = set()
                    df_counts[word].add(doc_id)
                
                # Теперь вычислим TF-IDF для каждого слова в каждом документе
                tf_idf_results = []
                
                # Сгруппируем по документам
                docs_dict = {}
                for item in stage2_results:
                    doc_id = item["key"]
                    (word, wordcount) = item["value"]
                    if doc_id not in docs_dict:
                        docs_dict[doc_id] = {"words": {}, "total_words": 0}
                    docs_dict[doc_id]["words"][word] = wordcount
                    docs_dict[doc_id]["total_words"] += wordcount
                
                # Вычислим TF-IDF
                for doc_id, doc_info in docs_dict.items():
                    total_words_in_doc = doc_info["total_words"]
                    for word, wordcount in doc_info["words"].items():
                        tf = wordcount / total_words_in_doc if total_words_in_doc > 0 else 0
                        df = len(df_counts.get(word, set()))
                        idf = math.log10(total_docs / df) if df > 0 else 0
                        tf_idf = tf * idf
                        
                        tf_idf_results.append({
                            "key": (word, doc_id),
                            "value": tf_idf
                        })
                
                self._reply(props, {"ok": True, "results": tf_idf_results})
            elif cmd == "get_document_vectors":
                # Вернуть TF-IDF векторы документов
                vectors = {}
                for doc_id, word_freq in self.word_freqs.items():
                    vectors[doc_id] = word_freq  # Здесь будет заменено на реальные TF-IDF векторы
                self._reply(props, {"ok": True, "vectors": vectors})
            elif cmd == "find_similar":
                # Найти документы, похожие на запрос
                query_vector = dict(req.get("query_vector") or {})
                doc_vectors = req.get("doc_vectors", {})
                
                results = []
                for doc_id, doc_vector in doc_vectors.items():
                    # Вычислить косинусное сходство
                    if doc_id in self.word_freqs:
                        similarity = self._calculate_cosine_similarity(
                            query_vector, 
                            self.word_freqs[doc_id]  # Здесь должен быть TF-IDF вектор
                        )
                        results.append({
                            "doc_id": doc_id,
                            "similarity": similarity
                        })
                
                # Отсортировать по убыванию сходства
                results.sort(key=lambda x: x["similarity"], reverse=True)
                self._reply(props, {"ok": True, "results": results})
            else:
                self._reply(props, {"ok": False, "error": f"unknown-cmd: {cmd}"})
        except Exception as e:
            self._reply(props, {"ok": False, "error": f"worker-error: {e}"})
        finally:
            ch.basic_ack(method.delivery_tag)

    def _calculate_cosine_similarity(self, vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
        """Вычислить косинусное сходство между двумя векторами"""
        # Найти общие слова
        common_words = set(vec1.keys()) & set(vec2.keys())
        
        # Вычислить скалярное произведение
        dot_product = sum(vec1[word] * vec2[word] for word in common_words)
        
        # Вычислить нормы векторов
        norm1 = sum(v ** 2 for v in vec1.values()) ** 0.5
        norm2 = sum(v ** 2 for v in vec2.values()) ** 0.5
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)

    def _send_to_replica(self, payload: dict):
        if self.role != "primary":
            return
        try:
            rq = WORKER_QUEUE_FMT.format(id=self.replica_id)
            self.ch.basic_publish(exchange="", routing_key=rq, body=json.dumps(payload).encode("utf-8"))
        except Exception:
            pass

    def run(self):
        print(f"[worker {self.worker_id}] started. AMQP={AMQP_URL}")
        try:
            while True:
                self.conn.process_data_events(time_limit=0.2)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self.conn.close()
            except Exception:
                pass

def main():
    ap = argparse.ArgumentParser("Worker (with simple mirroring to a replica)")
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--replica-id", type=int, required=True)
    ap.add_argument("--role", choices=["primary", "replica"], default="primary")
    args = ap.parse_args()
    w = Worker(args.id, args.replica_id, args.role)
    w.run()

if __name__ == "__main__":
    main()