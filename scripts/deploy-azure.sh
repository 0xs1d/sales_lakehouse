#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <infrastructure/*.bicepparam>" >&2
  exit 1
fi

PARAM_FILE="$1"
if [[ ! -f "$PARAM_FILE" ]]; then
  echo "Parameter file not found: $PARAM_FILE" >&2
  exit 1
fi

az deployment sub create \
  --location eastus \
  --template-file infrastructure/main.bicep \
  --parameters "$PARAM_FILE"
