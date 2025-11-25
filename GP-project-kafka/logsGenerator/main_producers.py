import threading
import time
import os
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import network_flows_producer
import auth_events_producer
import system_activity_producer
import wazuh_producer


def main():
    # ✅ Local Kafka connection with serializers
    producer = SerializingProducer({
        'bootstrap.servers': 'broker:29092',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': StringSerializer('utf_8')
    })

    # Optional: verify connection
    try:
        producer.list_topics(timeout=5)
        print("✅ Connected to Kafka broker at localhost:9092\n")
    except Exception as e:
        print("❌ Failed to connect to Kafka:", e)
        return

    # ✅ Fixed user dataset (deterministic)
    users = [
        {"username": "alice", "ip": "192.168.1.10", "hostname": "host-alice"},
        {"username": "bob", "ip": "192.168.1.11", "hostname": "host-bob"},
        {"username": "charlie", "ip": "192.168.1.12", "hostname": "host-charlie"},
    ]
    hosts = [u["hostname"] for u in users]

    # ✅ Configuration (controlled by orchestrator)
    duration = 60  # seconds total for each producer
    config = {
        "network": {"topic": "network_flows", "interval": 2},
        "auth": {"topic": "auth_events", "interval": 3},
        "system": {"topic": "system_activity", "interval": 4},
        "wazuh": {"topic": "wazuh_logs", "interval": 5},
    }

    print("🚀 Starting all producers...\n")

    # ✅ Create and start threads for each producer
    threads = [
        threading.Thread(
            target=network_flows_producer.run_network_producer,
            args=(producer, config["network"]["topic"], users, duration, config["network"]["interval"]),
            daemon=True
        ),
        threading.Thread(
            target=auth_events_producer.run_auth_producer,
            args=(producer, config["auth"]["topic"], users, duration, config["auth"]["interval"]),
            daemon=True
        ),
        threading.Thread(
            target=system_activity_producer.run_system_producer,
            args=(producer, config["system"]["topic"], hosts, duration, config["system"]["interval"]),
            daemon=True
        ),
        threading.Thread(
            target=wazuh_producer.run_wazuh_producer,
            args=(producer, config["wazuh"]["topic"], users, duration, config["wazuh"]["interval"]),
            daemon=True
        )
    ]

    # ✅ Start and join threads
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # ✅ Ensure buffered messages are delivered before exiting
    try:
        producer.flush(timeout=10)
        print("\n✅ All messages flushed successfully.")
    except Exception as e:
        print("⚠️ Kafka flush failed:", e)

    print("\n✅ All producers finished.")


if __name__ == "__main__":
    main()
