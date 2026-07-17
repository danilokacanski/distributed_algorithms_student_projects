# Tendermint Consensus Simulator

## Overview
This repository contains a distributed simulation of the Tendermint consensus algorithm. The system executes in a fully containerized environment, simulating the state machine transitions of an arbitrary, highly-scalable number of validator nodes communicating over a sparse gossip network. The network topology and scale can be easily customized via the provided Python generator.

## System Architecture

1. **Consensus Engine (Rust)**
   - Implements the core Tendermint state machine (`Propose`, `Prevote`, `Precommit`).
   - Utilizes `tokio` for asynchronous I/O and non-blocking TCP socket management.
   - Nodes communicate via a connected sparse gossip graph to minimize socket overhead while maintaining logarithmic network propagation.

2. **Telemetry Aggregator (Python)**
   - Acts as an Out-of-Band (OOB) monitoring service.
   - Receives non-blocking UDP telemetry packets from the consensus nodes.
   - Aggregates node state transitions and broadcasts them to clients via WebSockets.

3. **Presentation Layer (HTML/JS/CSS)**
   - A real-time, browser-based dashboard.
   - Visually maps the state of up to 56-node cluster, providing immediate feedback on quorum progression and Byzantine fault disruptions.

## Key Features
- **Byzantine Fault Injection**: Supports simulating malicious actors (e.g., `DoubleVote`, `Silent`, `SendInvalid`) to observe the robust $2f+1$ quorum resilience.
- **Artificial Network Latency**: Integrates Linux Traffic Control (`tc` via `iproute2`) to inject artificial packet delay directly at the container's ethernet interface, making it easy to observe consensus dynamics under slow-network conditions.
- **Dependency Isolation**: Everything is inside docker.

## Deployment Instructions

### Prerequisites
- Docker Engine
- Docker Compose (v2 with `buildx` is highly recommended)
- Python (required only for generating the initial topology)

### 1. Topology Generation
Prior to deployment, generate the network topology and configuration manifests:
```bash
python generate_compose.py
```
**Customization Options:**
You can manually edit the Python script to test different scenarios. The following configurations are natively supported:
- `num_nodes`: The total number of nodes in the cluster (e.g., `50`).
- `latency_ms`: Artificial network delay per hop in milliseconds (e.g., `500`).
- `behaviour`: The string determining a node's Byzantine fault logic. Supported values are:
  - `"standard"`: Honest node execution.
  - `"double-vote"`: Maliciously casts an extra vote for an invalid block to disrupt quorum.
  - `"silent"`: Simulates a crashed node by ignoring all incoming packets and refusing to broadcast.
  - `"send-invalid"`: Maliciously acts as a proposer for invalid block payloads.
- `rust_log`: `debug` for logs for every event/message, `info` for only state transitions,round starts, etc. 

### 2. Compilation and Initialization
Build the master container image and initialize the cluster. The Rust compiler executes within a multi-stage Dockerfile. To prevent your system from attempting to compile Rust for all nodes simultaneously, explicitly build the image for the first node (the rest will automatically clone it):
```bash
docker compose build node0
docker compose up -d
```

### 3. Monitoring and Visualization
Once the containers are actively computing consensus, the telemetry server will automatically begin bridging UDP packets. Open a web browser and navigate to the local presentation layer:

**http://localhost:8080**

### 4. Teardown
To safely terminate the simulation and prune the associated virtual bridge networks from the host:
```bash
docker compose down
```

## Future Work
While the current implementation successfully demonstrates $2f+1$ Byzantine fault tolerance in a static network, future iterations of this simulator could explore:
- **Crash Recovery & State Syncing**: Allowing a node that crashes to securely request and rebuild the consensus history from its peers upon rebooting.
- **Dynamic Validator Sets**: Implementing the ability to securely add or remove nodes from the active consensus pool while the network is running, without halting block production.
- **Cryptographic Signatures**: Implementing Ed25519 public-key cryptography to cryptographically sign and verify every proposal and vote, preventing malicious nodes from spoofing messages on behalf of honest peers.
