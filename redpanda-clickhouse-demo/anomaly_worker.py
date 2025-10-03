
import argparse, json, time, statistics, collections
from datetime import datetime
import requests
from kafka import KafkaConsumer


class Rolling:
    def __init__(self, maxlen=100):
        self.values = collections.deque(maxlen=maxlen)
    def add(self, x: float):
        try: self.values.append(float(x))
        except Exception: pass
    def mean(self): return statistics.mean(self.values) if self.values else 0.0
    def std(self):  return statistics.pstdev(self.values) if len(self.values) > 1 else 0.0
    def z(self, x: float):
        s = self.std(); m = self.mean()
        return 0.0 if s == 0 else (float(x) - m) / s


def call_ollama(model: str, payload: dict) -> str:
    """Local Ollama API (11434). Retry + uzun read timeout."""
    prompt = (
        "You are an SRE assistant. Explain briefly whether the following event "
        "is concerning and suggest 1-2 actions. Keep it under 3 bullet points.\n\n"
        f"Event JSON:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    url = "http://localhost:11434/api/generate"
    body = {"model": model, "prompt": prompt, "stream": False}

    last_err = None
    for attempt in range(3): 
        try:
            r = requests.post(url, json=body, timeout=(5, 120))  
            r.raise_for_status()
            return (r.json() or {}).get("response", "").strip()
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1)) 
    return f"(LLM unavailable) {last_err}"

def insert_alert(ch_host: str, ch_user: str, ch_pass: str, rows):
    """JSONEachRow ile toplu insert. Tarih parse için best_effort."""
    if not rows: return
    data = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    url = (
        f"http://{ch_user}:{ch_pass}@{ch_host}/"
        f"?date_time_input_format=best_effort"
        f"&input_format_skip_unknown_fields=1"
        f"&query=INSERT%20INTO%20default.alerts%20FORMAT%20JSONEachRow"
    )
    resp = requests.post(url, data=data.encode("utf-8"), timeout=15)
    if not resp.ok:
        raise RuntimeError(f"CH HTTP {resp.status_code}: {resp.text[:400]}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:19092")
    ap.add_argument("--topic", default="metrics")
    ap.add_argument("--threshold", type=float, default=85.0)
    ap.add_argument("--z", type=float, default=3.0)
    ap.add_argument("--window", type=int, default=100)
    ap.add_argument("--model", default="phi3:mini")
    ap.add_argument("--clickhouse", default="localhost:8123")
    ap.add_argument("--ch_user", default="default")
    ap.add_argument("--ch_pass", default="chpass")
    args = ap.parse_args()

    print(f"[worker] consuming {args.topic} from {args.brokers} model={args.model}", flush=True)

    
    try:
        _ = call_ollama(args.model, {"warmup": "ok"})
        print("[worker] ollama warmed", flush=True)
    except Exception:
        pass

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.brokers.split(","),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="anomaly-worker",
    )

    roll_cpu = collections.defaultdict(lambda: Rolling(args.window))
    roll_mem = collections.defaultdict(lambda: Rolling(args.window))

    batch = []
    last_flush = time.time()

    for msg in consumer:
        try:
            e = msg.value or {}
            svc = e.get("service", "unknown")
            host = e.get("host") or ""
            cpu = float(e.get("cpu_usage", 0.0))
            mem = float(e.get("mem_usage", 0.0))

            zc = roll_cpu[svc].z(cpu); roll_cpu[svc].add(cpu)
            zm = roll_mem[svc].z(mem); roll_mem[svc].add(mem)

            triggers = []
            if cpu > args.threshold: triggers.append(f"cpu>{args.threshold}")
            if mem > args.threshold: triggers.append(f"mem>{args.threshold}")
            if abs(zc) >= args.z:    triggers.append(f"|z_cpu|>={args.z:.1f}")
            if abs(zm) >= args.z:    triggers.append(f"|z_mem|>={args.z:.1f}")

            if triggers:
                rule = ",".join(triggers)
                ts_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                payload = {
                    "timestamp": e.get("timestamp"),
                    "service": svc, "host": host,
                    "cpu_usage": cpu, "mem_usage": mem,
                    "level": e.get("level"), "message": e.get("message"),
                    "rule": rule,
                }
                explanation = call_ollama(args.model, payload)

                row = {
                    "timestamp": ts_str,
                    "service": svc, "host": host,
                    "cpu_usage": cpu, "mem_usage": mem,
                    "level": e.get("level") or "",
                    "message": e.get("message") or "",
                    "rule": rule,
                    "score": float(max(abs(zc), abs(zm))),
                    "explanation": explanation,
                    "model": args.model,
                }
                print(f"[anomaly] {rule} svc={svc} cpu={cpu} mem={mem}", flush=True)
                batch.append(row)

            
            if batch and (time.time() - last_flush > 1.0):
                try:
                    insert_alert(args.clickhouse, args.ch_user, args.ch_pass, batch)
                    batch.clear(); last_flush = time.time()
                    print("[worker] alerts flushed to ClickHouse", flush=True)
                except Exception as ex:
                    print("[worker] CH insert failed:", ex, flush=True)
        except Exception as loop_ex:
            print("[worker] loop error:", loop_ex, flush=True)

if __name__ == "__main__":
    main()
