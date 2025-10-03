import json, os, random, string, time, argparse
from datetime import datetime, timezone
from kafka import KafkaProducer

SERVICES = ["auth", "payments", "search", "catalog", "orders", "api-gateway"]
LEVELS = ["INFO", "WARN", "ERROR"]

def rand_host():
    return "srv-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

def gen_record():
    service = random.choice(SERVICES)
    host = rand_host()
    cpu_base = random.uniform(5, 40)
    mem_base = random.uniform(20, 70)
    
    if random.random() < 0.02:
        cpu_base = random.uniform(85, 99)
        mem_base = random.uniform(85, 99)
        level = "ERROR"
        message = f"{service} anomaly detected: high CPU/MEM"
    else:
        level = random.choices(LEVELS, weights=[0.8, 0.15, 0.05])[0]
        message = f"{service} processed request"
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "host": host,
        "cpu_usage": round(cpu_base, 2),
        "mem_usage": round(mem_base, 2),
        "level": level,
        "message": message
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default=os.environ.get("BROKERS", "localhost:19092"))
    parser.add_argument("--topic", default=os.environ.get("TOPIC", "metrics"))
    parser.add_argument("--rate", type=float, default=float(os.environ.get("RATE", 5)), help="messages per second")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,
        acks="all",
    )
    print(f"Producing to {args.topic} at ~{args.rate} msg/s on {args.brokers}")
    interval = 1.0 / max(0.1, args.rate)
    try:
        while True:
            rec = gen_record()
            producer.send(args.topic, rec)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping.")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()