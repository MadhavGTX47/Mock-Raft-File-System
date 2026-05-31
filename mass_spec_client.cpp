#include <iostream>
#include <string>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <openssl/evp.h>

#include "raft.pb.h"

#pragma comment(lib, "ws2_32.lib")

// Calculates the SHA-256 hash of a string using OpenSSL EVP API
std::string calculateSHA256(const std::string& data) {
    if(data == "KILL") return "NONE";
    
    unsigned char hash[EVP_MAX_MD_SIZE]; 
    unsigned int length = 0;
    
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    EVP_DigestUpdate(ctx, data.c_str(), data.size());
    EVP_DigestFinal_ex(ctx, hash, &length);
    EVP_MD_CTX_free(ctx);

    std::ostringstream oss;
    for (unsigned int i = 0; i < length; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return oss.str();
}

int main() {
    std::cout << "--- Mass Spectrometer Interactive Client ---\n";
    
    // Initialize Windows Sockets
    WSADATA wsaData;
    int wsaResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (wsaResult != 0) {
        std::cerr << "Failed to initialize Winsock. Error code: " << wsaResult << "\n";
        return 1;
    }
    
    // Create UDP Socket
    SOCKET clientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (clientSocket == INVALID_SOCKET) {
        std::cerr << "Failed to create socket. Error: " << WSAGetLastError() << "\n";
        WSACleanup();
        return 1;
    }
    
    // Set 2-second receive timeout to detect leader failures
    DWORD timeout = 2000;
    setsockopt(clientSocket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));

    // Configure server address
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(8081); // Initial target: Node 1 (port 8081)
    inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);

    std::string currentLeader = "Node 1 (127.0.0.1:8081)";
    std::string userInput;

    while(true) {
        std::cout << "\n[Target: " << currentLeader << "]\n";
        std::cout << "Enter payload (or type 'KILL' to simulate leader crash, 'exit' to quit): ";
        std::getline(std::cin, userInput);

        if (userInput == "exit") break;
        if (userInput.empty()) continue;

        std::string finalPayload;
        if (userInput == "KILL") {
            finalPayload = "KILL";
        } else {
            // Compute data hash
            std::string hash = calculateSHA256(userInput);
            
            // Populate protobuf telemetry payload
            raft::TelemetryPayload payload;
            payload.set_timestamp(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
            payload.set_data_payload(userInput);
            payload.set_sha256_hash(hash);
            
            payload.SerializeToString(&finalPayload);
            std::cout << "[Client] Serialized Protobuf size: " << finalPayload.size() << " bytes\n";
        }

        std::cout << "[Client] Transmitting over UDP (Simulated gRPC Transport)...\n";
        sendto(clientSocket, finalPayload.c_str(), finalPayload.length(), 0, (SOCKADDR*)&serverAddr, sizeof(serverAddr));
        
        char buffer[1024];          
        sockaddr_in fromAddr;       
        int fromLen = sizeof(fromAddr);
        
        int bytesReceived = recvfrom(clientSocket, buffer, sizeof(buffer) - 1, 0, (SOCKADDR*)&fromAddr, &fromLen);

        if (bytesReceived > 0) {
            buffer[bytesReceived] = '\0';
            std::string reply(buffer);
            
            raft::TelemetryResponse response;
            if (response.ParseFromString(reply)) {
                // If contacted server is a Follower, redirect to current Leader
                if (!response.success() && !response.redirect_leader_ip().empty()) {
                    std::cout << "\n[CLIENT ALERT] Connection Rejected! Server is no longer the leader.\n";
                    std::cout << "[CLIENT ALERT] Parsing redirect... Updating internal routing table.\n";
                    
                    int newLeaderId = std::stoi(response.redirect_leader_ip());
                    int newPort = 8080 + newLeaderId;
                    
                    currentLeader = "Node " + std::to_string(newLeaderId) + " (127.0.0.1:" + std::to_string(newPort) + ")";
                    std::cout << "[CLIENT ALERT] New Leader is " << currentLeader << ". Future payloads will route here automatically.\n";
                    
                    serverAddr.sin_port = htons(newPort);
                    std::string newTargetIp = "127.0.0.1"; 
                    inet_pton(AF_INET, newTargetIp.c_str(), &serverAddr.sin_addr);
                } 
                else if (!response.success() && response.error_message() == "SERVER_CRASHED") {
                    std::cout << "[Client] Kill command sent. The Server cluster is currently panicking and re-electing...\n";
                }
                else if (response.success()) {
                    std::cout << "[Client] Server Response: HTTP 200 OK (Cluster Committed)\n";
                }
            } else {
                std::cout << "[Client Alert] Failed to parse response payload!\n";
            }
        } else {
            // Cycle to the next node in the cluster on timeout
            int currentPort = ntohs(serverAddr.sin_port);
            int nextNodeId = (currentPort - 8081 + 1) % 5 + 1;
            int nextPort = 8080 + nextNodeId;
            
            serverAddr.sin_port = htons(nextPort);
            currentLeader = "Node " + std::to_string(nextNodeId) + " (127.0.0.1:" + std::to_string(nextPort) + ")";
            std::cout << "[CLIENT ALERT] Request Timed Out! Switched target node port to failover node: " << currentLeader << "\n";
        }
    }

    closesocket(clientSocket);
    WSACleanup();
    return 0;
}
