import json, random, time
from datetime import datetime
from confluent_kafka import SerializingProducer

def delivery_report(err, msg):
    if err:
        print(f"[NETWORK] ❌ Delivery failed: {err}")
    else:
        print(f"[NETWORK] ✅ Sent to {msg.topic()} [{msg.partition()}]")

def run_network_producer(producer, topic, users, duration, interval):
    start = time.time()
    while time.time() - start < duration:
        user = random.choice(users)
        record = {
            "user": user["username"],
            "src_ip": user["ip"],
            "dst_ip": random.choice(["10.0.0.5", "172.16.0.10", "192.168.1.12"]),
            "protocol": random.choice(["TCP", "UDP"]),
            "dst_port": random.choice([22, 80, 443]),
            "bytes_sent": random.randint(200, 10000),
            "bytes_received": random.randint(200, 10000),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # produce to Kafka, use host as key so events for same host go to same partition
        try:
            # use the user's hostname as the key so events for the same host go to the same partition
            producer.produce(
                topic=topic,
                key=user["hostname"],
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
