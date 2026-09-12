# Counter Example

A stateful counter actor demonstrating Miniate's template registration, actor lifecycle, and auto-resume via the ingress router.

## Quickstart

### 1. Deploy
```bash
./examples/counter/deploy.sh
```

### 2. Send Traffic
```bash
curl -X POST \
  -H "ate-target-actor: demo/counter-1" \
  http://localhost:8000/
```

### 3. Test Suspend & Auto-Resume
Suspend the actor to release worker slots:
```bash
miniate actor suspend counter-1 -a demo
```

Send another request (the router auto-resumes the actor and preserves its count):
```bash
curl -X POST \
  -H "ate-target-actor: demo/counter-1" \
  http://localhost:8000/
```

### 4. Cleanup
```bash
./examples/counter/cleanup.sh
```
