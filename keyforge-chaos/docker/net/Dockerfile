# The network side of a chaos-test node. One container of this image owns each node's network namespace (its address,
# its DNS name kfN, its published ports); the KeyForge server runs in a second container that joins the namespace
# (docker-compose.yml: network_mode: service:kfN-net). Killing or pausing the server therefore leaves the network
# namespace, and the faults installed in it, in place, and the nemesis always has a running container to drive:
#
# - iptables: partitions (drop packets from chosen peers) and packet loss (the statistic match drops a random share);
# - Toxiproxy: every inbound Raft connection is redirected through a local proxy, whose latency toxic delays it.
#
# tc netem would be the usual tool for latency and loss, but the Docker Desktop (WSL 2) kernel is built without it.
FROM ghcr.io/shopify/toxiproxy:2.12.0 AS toxiproxy

FROM alpine:3.22
RUN apk add --no-cache iptables iproute2
COPY --from=toxiproxy /toxiproxy /usr/local/bin/toxiproxy-server
COPY --from=toxiproxy /toxiproxy-cli /usr/local/bin/toxiproxy-cli
COPY net-init.sh /usr/local/bin/net-init
RUN chmod +x /usr/local/bin/net-init
ENTRYPOINT ["/usr/local/bin/net-init"]
