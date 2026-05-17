# TCP-over-UDP Reliable Transport Sandbox

A C++ project that simulates reliable data transfer protocols (TCP-like) over an unreliable UDP layer. This project was developed to demonstrate a deep understanding of network protocols and transport-layer mechanisms.

## Implemented Network Concepts
- **Reliable Data Transfer**: Implemented custom packet headers with Sequence (SEQ) and Acknowledgment (ACK) numbers.
- **Congestion Control**: Simulates Slow Start, Congestion Avoidance, and Fast Recovery algorithms.
- **Error Control**: Supports Fast Retransmit (via 3 duplicate ACKs) and timeout detection.
- **Concurrency**: Server architecture utilizes multi-processing (`fork`) and multi-threading (`pthread`) to handle network I/O operations asynchronously.

## Environment
- Originally designed and tested in Ubuntu (Linux) environment using `g++` and standard POSIX socket APIs.
