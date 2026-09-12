# Counter Agent Example

This example demonstrates how to define, deploy, and interact with a stateful agent on **Miniate**.

## Files

- **`template.yaml`**: Standard Substrate `ActorTemplate` manifest configuring container image, readiness probe, resource limits, snapshotting policy (`FULL`), and sandbox class (`gvisor`).
- **`main.go`**: Standalone Go HTTP actor maintaining state across invocations.
- **`deploy.sh`**: Automated script to start Miniate, register the template, spin up actors, send requests with auto-resume, and demonstrate hibernation.
- **`cleanup.sh`**: Script to tear down the example atespace and actors.

---

## Quickstart

### 1. Deploy the Agent
Run the deployment script:
```bash
./examples/counter-agent/deploy.sh
```

### 2. Send Traffic
Send requests through the `atenet` smart router (port `8000`):

```bash
curl -X POST \
  -H "ate-target-actor: demo/counter-1" \
  http://localhost:8000/
```

### 3. Test Auto-Hibernation & Wake-Up
Suspend the actor to release physical worker pool slots and capture a snapshot:

```bash
miniate actor suspend counter-1 -a demo
```

Next, send another request:
```bash
curl -X POST \
  -H "ate-target-actor: demo/counter-1" \
  http://localhost:8000/
```
The router will automatically detect the suspended state, resume the actor in `<1ms`, and restore its counter without dropping traffic.

### 4. Open Web Dashboard
View live worker slot assignments and logs at:
[http://localhost:8082/dashboard](http://localhost:8082/dashboard)

### 5. Cleanup
```bash
./examples/counter-agent/cleanup.sh
```
