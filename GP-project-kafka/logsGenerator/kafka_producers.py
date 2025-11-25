import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker and Kafka Producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:29092', # Use localhost if running script from host, use 'kafka:9092' if inside docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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