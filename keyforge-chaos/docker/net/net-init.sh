#!/bin/sh
# Prepares a node's network namespace, then keeps it alive (see the Dockerfile).
#
# Inbound connections to the Raft port are redirected to a Toxiproxy proxy named "raft", which forwards them to the
# server on the loopback interface. Without toxics the proxy only relays bytes. The REDIRECT rule lives in the nat
# table; the nemesis adds and flushes its own rules in the filter table's INPUT chain only.
set -eu
RAFT_PORT=${RAFT_PORT:-7000}
PROXY_PORT=${PROXY_PORT:-17000}

toxiproxy-server -host 127.0.0.1 -port 8474 >/dev/null &
server=$!
trap 'kill "$server"' TERM INT
until toxiproxy-cli list >/dev/null 2>&1; do sleep 0.1; done
toxiproxy-cli create -l "0.0.0.0:$PROXY_PORT" -u "127.0.0.1:$RAFT_PORT" raft >/dev/null
iptables -t nat -A PREROUTING -p tcp --dport "$RAFT_PORT" -j REDIRECT --to-ports "$PROXY_PORT"
echo "net-init: Raft port $RAFT_PORT is proxied through Toxiproxy on $PROXY_PORT"
wait "$server"
