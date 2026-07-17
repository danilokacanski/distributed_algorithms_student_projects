#!/bin/bash

# Apply traffic control delay if LATENCY_MS is set
if [ ! -z "$LATENCY_MS" ] && [ "$LATENCY_MS" -gt 0 ]; then
    echo "Adding ${LATENCY_MS}ms network latency to eth0..."
    tc qdisc add dev eth0 root netem delay ${LATENCY_MS}ms
fi

# Execute the main Rust binary
exec ./tendermint-sim
