# Miniate 🪄

**Miniate** is a lightweight local development environment and CLI tool for **[Agent Substrate](https://github.com/agent-substrate/substrate)**, designed to provide the same effortless, zero-friction developer experience that `minikube` brings to Kubernetes.

Miniate replicates Substrate's entire control plane API surface (`ateapipb.Control` and `ateapipb.WorkerService`), embeds a local worker execution and multiplexing pool, includes a traffic router (`atenet`) with automatic actor hibernation and resume, and serves a live developer web dashboard.


## Features

- **Substrate API Compatible**: Replicates the Agent Substrate Control Plane APIs and the Router.
- **Local Worker Runtime**: Multiplexes stateful actors across local workers.
- **Traffic Router**: Ingress router with sub-millisecond auto-resume for suspended actors.
- **State Persistence & Snapshots**: Preserves state across hibernation cycles with local snapshot tracking.
- **CLI & Web Dashboard**: Developer CLI and a live browser UI (`:8082/dashboard`).


## Overview

```
                      ┌──────────────────┐
                      │      Client      │
                      └───┬──────────┬───┘
             HTTP :8000   │          │  gRPC :8080
                          ▼          ▼
                 ┌─────────────┐ ┌───────────────┐
                 │   Router    │ │ Control Plane │
                 └──────┬──────┘ └───────┬───────┘
                        │ Auto-Resume    │ RPCs
                        ▼                ▼
                 ┌───────────────────────────────┐
                 │        Runtime Engine         │
                 │  • Worker Slots (worker-0..7) │
                 │  • State & Snapshot Store     │
                 │  • Embedded Dashboard (:8082) │
                 └───────────────────────────────┘
```

## Quickstart

### 1. Installation

```bash
make install
```

### 2. Start the Local Substrate Cluster

Start the local background daemon:
```bash
miniate start
```
*Outputs:*
```
🚀 Miniate started successfully in background!
   Control plane: localhost:8080
   Traffic Router: http://localhost:8000
   Dashboard:     http://localhost:8082/dashboard
   Workers:       8 slots
```

Check cluster status:
```bash
miniate status
```

### 3. Create an Atespace, Template, and Actor

```bash
# 1. Create an isolation atespace
miniate atespace create demo

# 2. Create an actor template
miniate template create counter -a demo

# 3. Create a stateful actor (initial state: SUSPENDED)
miniate actor create my-counter-1 -a demo --template counter

# List actors
miniate actor list -A
```

### 4. Send Traffic and Test Auto-Resume

Send an HTTP request through the smart router (`atenet` on port 8000). The router automatically wakes up the suspended actor onto a free worker slot, buffers the request during activation, and forwards it to the actor:

```bash
curl -X POST \
  -H "ate-target-actor: demo/my-counter-1" \
  http://localhost:8000/
```

*Response:*
```json
{
  "atespace": "demo",
  "actor": "my-counter-1",
  "template": "counter",
  "worker": "worker-7",
  "count": 1,
  "version": 2,
  "timestamp": "2026-09-12T12:24:48-07:00"
}
```

### 5. Suspend, Resume, and Inspect Logs

```bash
# Suspend the actor (frees the worker slot and persists snapshot)
miniate actor suspend my-counter-1 -a demo

# View actor logs
miniate actor logs my-counter-1 -a demo

# Inspect physical workers
miniate worker list
```

### 6. Open Web Dashboard

Navigate to **`http://localhost:8082/dashboard`** in your browser to view:
- Physical worker slots matrix (assigned vs free)
- Active actors table with instant **Resume** / **Suspend** / **Delete** buttons
- Multiplexing ratio (e.g. 30x oversubscription)
- Live log tailing
- Interactive endpoint helper


## Command Reference

| Command | Description |
|---|---|
| `miniate start` | Start the local control plane daemon (`--foreground`, `--workers`, `--grpc-port`, `--router-port`, `--dashboard-port`) |
| `miniate stop` | Stop the running daemon |
| `miniate status` | Inspect cluster health, workers, and actor counts |
| `miniate delete` | Delete local state and snapshots |
| `miniate dashboard` | Print dashboard URL |
| `miniate atespace create` | Create an atespace boundary |
| `miniate atespace list` | List existing atespaces |
| `miniate atespace delete` | Delete an atespace |
| `miniate template create` | Create an actor template (`-f template.yaml` or JSON) |
| `miniate template list` | List actor templates |
| `miniate template delete` | Delete an actor template |
| `miniate actor create` | Create a stateful actor instance from a template |
| `miniate actor list` | List actors across namespaces |
| `miniate actor get` | Inspect actor status and worker assignments |
| `miniate actor resume` | Resume a suspended actor onto a worker slot |
| `miniate actor suspend` | Suspend a running actor and snapshot state |
| `miniate actor pause` | Pause an actor without releasing worker slot |
| `miniate actor delete` | Delete an actor |
| `miniate actor logs` | Tail logs for an actor |
| `miniate worker list` | List physical worker slots and status |
| `miniate worker get` | Get detailed worker capacity and allocations |
| `miniate worker drain` | Drain an active worker slot |
| `miniate tag create` | Create a named tag for an actor snapshot |
| `miniate tag list` | List snapshot tags |
| `miniate tag get` | Inspect a specific tag |
| `miniate tag delete` | Delete a tag |
