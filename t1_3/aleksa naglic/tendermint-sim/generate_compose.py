import yaml
import random

NUM_NODES = 56
PEERS_OUT = 7

services = {}

for i in range(NUM_NODES):
    peers = []
    for j in range(1, PEERS_OUT + 1):
        target = (i + j) % NUM_NODES
        target_port = 8000 + target
        peers.append(f"{target}:node{target}:{target_port}")
    
    peers_str = ",".join(peers)
    behaviour = "standard"
    
    if i in [9, 37]:
        behaviour = "double-vote"
    if i in [18, 46]:
        behaviour = "send-invalid"
    if i in [27, 55]:
        behaviour = "silent"

    latency = random.randint(100, 300)
    
    if i == 0:
        services["monitor"] = {
            "build": {
                "context": ".",
                "dockerfile": "Dockerfile.monitor"
            },
            "ports": [
                "8080:8080",
                "8081:8081"
            ],
            "networks": ["tm-net"]
        }

    services[f"node{i}"] = {
        "build": ".",
        "image": "tendermint-sim-image",
        "cap_add": ["NET_ADMIN"],
        "environment": [
            f"NODE_ID={i}",
            f"TOTAL_NODES={NUM_NODES}",
            f"PEERS={peers_str}",
            "RUST_LOG=info",
            f"BEHAVIOR={behaviour}",
            f"LATENCY_MS={latency}"
        ],
        "networks": ["tm-net"]
    }

compose = {
    "services": services,
    "networks": {
        "tm-net": {
            "driver": "bridge"
        }
    }
}

with open("docker-compose.yml", "w") as f:
    yaml.dump(compose, f, sort_keys=False)

print(f"Successfully generated docker-compose.yml for {NUM_NODES} nodes with {PEERS_OUT} peers each")