import json, random, time
from datetime import datetime
from confluent_kafka import SerializingProducer

def delivery_report(err, msg):
    if err:
        print(f"[WAZUH] ❌ Delivery failed: {err}")
    else:
        print(f"[WAZUH] ✅ Sent to {msg.topic()} [{msg.partition()}]")

def run_wazuh_producer(producer, topic, users, duration, interval):
    start = time.time()
    while time.time() - start < duration:
        user = random.choice(users)
        record = {
            "agent": user["hostname"],
            "rule_id": random.randint(1000, 9999),
            "severity": random.randint(1, 10),
            "category": random.choice(["network_scan", "privilege_escalation", "malware_detected"]),
            "src_ip": user["ip"],
            "dst_ip": random.choice(["10.0.0.5", "192.168.1.25"]),
            "description": "Suspicious network pattern detected",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # produce to Kafka, use host as key so events for same host go to same partition
        try:
            # use the agent (hostname) as the message key so events for the same agent go to same partition
            producer.produce(
                topic=topic,
                key=record["agent"],
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
