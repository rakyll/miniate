#!/usr/bin/env bash
# Deploy Counter Agent to Miniate Local Cluster
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIATE_BIN="${SCRIPT_DIR}/../../bin/miniate"

# Fall back to PATH if local build binary not found
if [ ! -f "${MINIATE_BIN}" ]; then
  MINIATE_BIN="miniate"
fi

echo "=========================================================="
echo "Deploying Example Counter Actor to Miniate"
echo "=========================================================="

# 1. Ensure Miniate cluster is running
echo "1. Checking Miniate cluster status..."
if ! ${MINIATE_BIN} status >/dev/null 2>&1; then
  echo "   Starting Miniate daemon..."
  ${MINIATE_BIN} start
  sleep 1
else
  echo "   Miniate cluster is already running."
fi

# 2. Create the demo atespace
echo ""
echo "2. Ensuring 'demo' atespace exists..."
${MINIATE_BIN} atespace create demo 2>/dev/null || true

# 3. Create the Actor Template from YAML
echo ""
echo "3. Registering actor template 'counter' from template.yaml..."
${MINIATE_BIN} template create -f "${SCRIPT_DIR}/template.yaml" -a demo 2>/dev/null || true

# 4. Create Actor instances (stateful actors)
echo ""
echo "4. Creating stateful actors..."
${MINIATE_BIN} actor create counter-1 -a demo --template counter 2>/dev/null || true
${MINIATE_BIN} actor create counter-2 -a demo --template counter 2>/dev/null || true

echo ""
echo "5. Current Actors in cluster:"
${MINIATE_BIN} actor list -A

# 5. Send traffic through Traffic Router (atenet on :8000)
echo ""
echo "=========================================================="
echo "Testing Smart Traffic Ingress & Auto-Resume"
echo "=========================================================="
echo "Sending request 1 to demo/counter-1 (triggers auto-resume from SUSPENDED)..."
curl -s -X POST -H "ate-target-actor: demo/counter-1" http://localhost:8000/
echo ""

echo ""
echo "Sending request 2 to demo/counter-1 (routed to active worker slot)..."
curl -s -X POST -H "ate-target-actor: demo/counter-1" http://localhost:8000/
echo ""

echo ""
echo "Sending request 1 to demo/counter-2..."
curl -s -X POST -H "ate-target-actor: demo/counter-2" http://localhost:8000/
echo ""

echo ""
echo "=========================================================="
echo "Testing Suspend & Snapshot Capture"
echo "=========================================================="
echo "Suspending demo/counter-1 (frees worker slot and saves snapshot)..."
${MINIATE_BIN} actor suspend counter-1 -a demo

echo ""
echo "Actors state after suspend:"
${MINIATE_BIN} actor list -a demo

echo ""
echo "Waking demo/counter-1 back up via HTTP ingress request..."
curl -s -X POST -H "ate-target-actor: demo/counter-1" http://localhost:8000/ | grep -o '"count":[0-9]*' || true
echo ""

echo "=========================================================="
echo "Deployment & Verification Complete!"
echo "   Live Dashboard: http://localhost:8082/dashboard"
echo "   Actor Logs:     ${MINIATE_BIN} actor logs counter-1 -a demo"
echo "   Cleanup:        ${SCRIPT_DIR}/cleanup.sh"
echo "=========================================================="
