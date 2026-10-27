#!/usr/bin/env bash
# Codespaces' docker-in-docker leaves legacy iptables tables with FORWARD policy DROP that
# only allow docker0. With bridge-nf-call-iptables=1 they also filter the compose network:
# container-to-container traffic (datanode -> namenode, airflow -> HDFS) and outbound
# internet (spark-submit --packages downloading the JDBC driver) are silently dropped.
# Restore normal Docker bridge behavior for the compose bridge (name fixed in
# docker-compose.yml): traffic inside it, outgoing connections, and replies to them.
# New incoming connections from outside stay blocked.
set -euo pipefail

BRIDGE=jobads0
RULES=(
  "-i $BRIDGE -o $BRIDGE -j ACCEPT"
  "-i $BRIDGE ! -o $BRIDGE -j ACCEPT"
  "-o $BRIDGE -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT"
)

if ! command -v iptables-legacy >/dev/null; then
  echo "iptables-legacy not found, nothing to do"
  exit 0
fi

for rule in "${RULES[@]}"; do
  # shellcheck disable=SC2086  # rule is a list of iptables arguments
  if sudo iptables-legacy -C FORWARD $rule 2>/dev/null; then
    echo "legacy iptables: already present: $rule"
  else
    sudo iptables-legacy -I FORWARD $rule
    echo "legacy iptables: added: $rule"
  fi
done
