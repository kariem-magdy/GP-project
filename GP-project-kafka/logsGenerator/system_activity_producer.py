import json, random, time
from datetime import datetime
from confluent_kafka import SerializingProducer

def delivery_report(err, msg):
    if err:
        print(f"[SYSTEM] ❌ Delivery failed: {err}")
    else:
        print(f"[SYSTEM] ✅ Sent to {msg.topic()} [{msg.partition()}]")

def run_system_producer(producer, topic, hosts, duration, interval):
    start = time.time()
    while time.time() - start < duration:
        host = random.choice(hosts)
        record = {
            "host": host,
            "cpu_usage": round(random.uniform(1.0, 85.0), 2),
            "memory_usage": round(random.uniform(5.0, 90.0), 2),
            "disk_usage": round(random.uniform(10.0, 95.0), 2),
            "load_avg_1m": round(random.uniform(0.0, 4.0), 2),
            "net_bytes_sent": random.randint(1024, 10_000_000),
            "net_bytes_recv": random.randint(1024, 10_000_000),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        
        # produce to Kafka, use host as key so events for same host go to same partition
        try:
            producer.produce(
                topic=topic,
                key=record["host"],
                value=json.dumps(record),
                on_delivery=delivery_report
            )
            producer.poll(0)
        except BufferError:
            print("[SYSTEM] Buffer full, waiting 1s")
            time.sleep(1)
            continue
        except Exception as e:
            print(f"[SYSTEM] Exception producing message: {e}")

        # sleep for the configured interval before next emission
        time.sleep(interval)
