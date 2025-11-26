import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker

# Initialize Faker
fake = Faker()

# Initialize Kafka Producer with retry logic
def create_kafka_producer(max_retries=30, retry_delay=2):
    """Create Kafka producer with retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 5, 0),
                request_timeout_ms=30000,
                max_block_ms=30000
            )
            print("Successfully connected to Kafka!")
            return producer
        except NoBrokersAvailable as e:
            print(f"Kafka not ready yet: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to connect to Kafka after all retries")
                raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

producer = create_kafka_producer()

# --- CONFIGURATION ---
TOPICS = {
    "wazuh": "wazuh_alerts",
    "keycloak": "keycloak_events",
    "haproxy": "haproxy_logs",
    "prometheus": "prometheus_metrics"
}

# Simulated Users (Pre-assigned IDs to link logs across systems)
USERS = [
    {"id": "user-001", "name": "karim", "role": "admin", "ip": "192.168.1.10"},
    {"id": "user-002", "name": "sara", "role": "analyst", "ip": "192.168.1.11"},
    {"id": "user-003", "name": "malicious_insider", "role": "dev", "ip": "192.168.1.12"}
]

def get_time_now():
    return datetime.now(timezone.utc).isoformat()

# --- GENERATORS ---

def gen_keycloak(user):
    """Generates Identity/Auth events"""
    event_type = random.choices(
        ["LOGIN", "LOGIN_ERROR", "REFRESH_TOKEN", "LOGOUT"], 
        weights=[0.7, 0.05, 0.2, 0.05]
    )[0]
    
    msg = {
        "time": int(time.time() * 1000), # Flink likes epoch millis
        "type": event_type,
        "realmId": "corp-realm",
        "clientId": "portal-app",
        "userId": user["id"],
        "ipAddress": user["ip"] if event_type != "LOGIN_ERROR" else fake.ipv4(), # Error might come from strange IP
        "error": "invalid_credentials" if event_type == "LOGIN_ERROR" else None,
        "details": {"username": user["name"]}
    }
    producer.send(TOPICS["keycloak"], msg)
    print(f"[Keycloak] Sent {event_type} for {user['name']}")

def gen_haproxy(user):
    """Generates API Gateway traffic"""
    path = random.choice(["/api/v1/data", "/api/v1/user", "/api/v1/admin", "/login", "/static/css"])
    method = random.choice(["GET", "POST", "DELETE"])
    status = random.choices([200, 401, 403, 500], weights=[0.9, 0.05, 0.03, 0.02])[0]
    
    msg = {
        "timestamp": get_time_now(),
        "client_ip": user["ip"],
        "jwt_user_id": user["id"], # EXTRACTED FROM TOKEN
        "http_method": method,
        "http_path": path,
        "http_status": status,
        "user_agent": "Mozilla/5.0...",
        "bytes_read": random.randint(100, 5000)
    }
    producer.send(TOPICS["haproxy"], msg)
    print(f"[HAProxy] Sent {method} {path} ({status}) for {user['name']}")

def gen_wazuh(user):
    """Generates Endpoint Security Alerts"""
    # Only generate alert 20% of the time
    if random.random() > 0.2: return

    rule_id = random.choice(["5715", "5710", "550", "10000"])  # SSH Success, SSH Fail, File Mod, Mimikatz
    level = 3 if rule_id == "5715" else random.randint(5, 12)
    
    msg = {
        "timestamp": get_time_now(),
        "rule": {
            "level": level,
            "id": rule_id,
            "description": "Simulated Security Event",
            "groups": ["syslog", "sshd"]
        },
        "agent": {"id": "001", "name": f"agent-{user['name']}"},
        "userId": user["id"], # ENRICHED FIELD
        "decoder": {"name": "sshd"},
        "full_log": f"Simulated log for rule {rule_id}"
    }
    producer.send(TOPICS["wazuh"], msg)
    print(f"[Wazuh] Alert Level {level} for {user['name']}")

def gen_prometheus(user):
    """Generates System Metrics linked to User's Pod/Container"""
    metrics = ["agent_cpu_avg", "agent_mem_avg", "agent_net_out_sum_bytes"]
    
    for m in metrics:
        msg = {
            "timestamp": int(time.time() * 1000),
            "metric_name": m,
            "value": random.uniform(0.1, 100.0),
            "labels": {
                "userId": user["id"], # LABELED IN PROMETHEUS
                "container": "user-service"
            }
        }
        producer.send(TOPICS["prometheus"], msg)

# --- MAIN LOOP ---
print("Starting Log Simulation...")
try:
    while True:
        for user in USERS:
            gen_keycloak(user)
            gen_haproxy(user)
            gen_wazuh(user)
            gen_prometheus(user)
        time.sleep(2) # Wait 2 seconds between batches
except KeyboardInterrupt:
    print("Stopping producers.")