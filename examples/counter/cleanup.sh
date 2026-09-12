#!/usr/bin/env bash
# Cleanup Counter Agent Demo from Miniate Cluster
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIATE_BIN="${SCRIPT_DIR}/../../bin/miniate"

if [ ! -f "${MINIATE_BIN}" ]; then
  MINIATE_BIN="miniate"
fi

echo "Cleaning up example counter agent resources..."

${MINIATE_BIN} actor delete counter-1 -a demo 2>/dev/null || true
${MINIATE_BIN} actor delete counter-2 -a demo 2>/dev/null || true
${MINIATE_BIN} template delete counter -a demo 2>/dev/null || true
${MINIATE_BIN} atespace delete demo 2>/dev/null || true

echo "Example resources cleaned up successfully."
