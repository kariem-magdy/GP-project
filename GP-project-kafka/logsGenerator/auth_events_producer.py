import json, random, time
from datetime import datetime
from confluent_kafka import SerializingProducer

def delivery_report(err, msg):
    if err:
        print(f"[AUTH] ❌ Delivery failed: {err}")
    else:
        print(f"[AUTH] ✅ Sent to {msg.topic()} [{msg.partition()}]")

def run_auth_producer(producer, topic, users, duration, interval):
    start = time.time()
    while time.time() - start < duration:
        user = random.choice(users)
        record = {
            "username": user["username"],
            "ip": user["ip"],
            "action": random.choice(["login_success", "login_failure"]),
            "method": random.choice(["password", "2FA"]),
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
