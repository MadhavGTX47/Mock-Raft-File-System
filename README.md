# Secure Distributed File System (Raft Consensus Prototype)

## Overview
This prototype was developed as a distributed systems architectural proof-of-concept. The goal is to offload heavy mass spectrometry data processing (massive HDF5 files) from the primary hardware acquisition PC to a locally-hosted 5-node server cluster. 

By streaming data immediately to the cluster, the acquisition PC is freed from UI rendering and disk I/O bottlenecks, eliminating hardware latency spikes and preventing the digitizer buffers from overflowing.

## File Structure
* **`raft.proto`**: The official Protocol Buffer schema defining the client-to-server and server-to-server RPC interfaces.
* **`raft.pb.h`**: The mock-generated Protobuf header that implements serialization (`SerializeToString`) and deserialization (`ParseFromString`). This allows the C++ codebase to use standard Protobuf getter/setter APIs without the massive runtime library dependencies.
* **`raft_server.cpp`**: The cluster node service. It can run as a single process spawning 5 virtual nodes (threads) on ports 8081-8085, or as 5 standalone processes communicating via real UDP socket messages (heartbeats, elections, voting).
* **`mass_spec_client.cpp`**: The acquisition client. It calculates SHA-256 hashes of the spectral data, packages them into a `TelemetryPayload` Protobuf object, encrypts the stream using AES-256-CBC, and transmits it. It features self-healing routing and failover detection.

## Technologies Used
* **C++17/20**: Core execution logic and multithreading.
* **Raft Consensus**: Handles leader election and data log replication across the 5 nodes to guarantee data survival in case of hardware failure or network partitions.
* **Protocol Buffers**: Custom mock implementation allows for highly optimized binary serialization of telemetry without dependencies.
* **OpenSSL (AES-256-CBC & SHA-256)**: AES-256-CBC encryption secures all node-to-node and client-to-node packets. SHA-256 hashing on all payloads guarantees absolute file integrity against industrial electromagnetic line noise (which standard 16-bit TCP checksums can miss).

## Architecture Flow
1. **The Client (`mass_spec_client.cpp`)**: Simulates the host PC. It generates a chunk of data, hashes it via OpenSSL, serializes it to a `TelemetryPayload` binary format, encrypts it using AES-256-CBC, and pushes it to the current Raft Leader via UDP.
2. **The Server Nodes (`raft_server.cpp`)**: A cluster of 5 nodes listening on ports 8081-8085. The current leader regularly broadcasts encrypted heartbeats (`AppendEntriesRequest`). If the leader is killed (via client command) or isolated, the followers detect the heartbeat loss via randomized election timers, transition to Candidate, hold an election, and promote a new leader.
3. **Self-Healing Failover**: If the client times out on a dead leader port, it cycles to the next node port. If it contacts a follower, the follower responds with a `TelemetryResponse` containing a redirect leader IP. The client updates its address structure and routes all future traffic to the new leader port automatically.

## Simulation Commands
In addition to telemetry streaming, you can execute control commands from the client terminal to test node states:
* **`KILL`**: Simulates a complete server process crash on the current leader.
* **`ISOLATE <nodeId>`**: Simulates a network partition by cutting off all communication to/from the target node (it drops incoming packets and cannot send outgoing packets).
* **`HEAL <nodeId>`**: Restores the network connection of the target node, verifying Raft network healing (e.g. an isolated leader stepping down upon seeing a higher term).

## Compilation

### Windows (MSVC/MinGW)
Ensure OpenSSL and Winsock development headers are available. Compile with:
```powershell
# Compile the client
g++ -std=c++17 mass_spec_client.cpp -lssl -lcrypto -lws2_32 -o client.exe

# Compile the server
g++ -std=c++17 raft_server.cpp -lssl -lcrypto -lws2_32 -o server.exe
```

### Linux / WSL
```bash
# Compile the client
g++ -std=c++17 mass_spec_client.cpp -lssl -lcrypto -o client

# Compile the server
g++ -std=c++17 raft_server.cpp -lssl -lcrypto -lpthread -o server
```

## Running the Simulation

You can run this project in two modes:

### Mode A: Multi-Threaded Simulation (Single Console Window)
To spin up all 5 nodes on ports 8081-8085 inside a single process:
1. Run `.\server.exe` (no command line arguments).
2. Open another console and run `.\client.exe`.

### Mode B: Standalone Processes (Multiple Console Windows)
To simulate a true multi-machine network where nodes run as separate processes:
1. Open 5 terminal windows.
2. Run `.\server.exe 1` in window 1, `.\server.exe 2` in window 2, etc. Each node binds to `8080 + NodeID` and communicates over local sockets.
3. Open a 6th window and run `.\client.exe`.
