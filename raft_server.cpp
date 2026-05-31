#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <random>
#include <string>
#include <sstream>
#include <map>
#include <iomanip>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <openssl/evp.h>

#pragma comment(lib, "ws2_32.lib")

#include "raft.pb.h"

const std::string AES_KEY = "01234567890123456789012345678901";
const std::string AES_IV  = "0123456789012345";

std::mutex printMtx;
void safePrint(const std::string& msg) {
    std::lock_guard<std::mutex> lock(printMtx);
    std::cout << msg << std::endl;
}

std::string calculateSHA256(const std::string& data) {
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

std::string encryptAES(const std::string& plaintext, const std::string& key, const std::string& iv) {
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, (const unsigned char*)key.c_str(), (const unsigned char*)iv.c_str());
    
    std::string ciphertext;
    ciphertext.resize(plaintext.size() + EVP_CIPHER_block_size(EVP_aes_256_cbc()));
    
    int len = 0;
    EVP_EncryptUpdate(ctx, (unsigned char*)&ciphertext[0], &len, (const unsigned char*)plaintext.c_str(), plaintext.size());
    int ciphertext_len = len;
    
    EVP_EncryptFinal_ex(ctx, (unsigned char*)&ciphertext[0] + len, &len);
    ciphertext_len += len;
    
    ciphertext.resize(ciphertext_len);
    EVP_CIPHER_CTX_free(ctx);
    
    return ciphertext;
}

std::string decryptAES(const std::string& ciphertext, const std::string& key, const std::string& iv) {
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, (const unsigned char*)key.c_str(), (const unsigned char*)iv.c_str());
    
    std::string plaintext;
    plaintext.resize(ciphertext.size());
    
    int len = 0;
    EVP_DecryptUpdate(ctx, (unsigned char*)&plaintext[0], &len, (const unsigned char*)ciphertext.c_str(), ciphertext.size());
    int plaintext_len = len;
    
    int ret = EVP_DecryptFinal_ex(ctx, (unsigned char*)&plaintext[0] + len, &len);
    if (ret <= 0) {
        EVP_CIPHER_CTX_free(ctx);
        return "";
    }
    plaintext_len += len;
    
    plaintext.resize(plaintext_len);
    EVP_CIPHER_CTX_free(ctx);
    
    return plaintext;
}

enum class State { FOLLOWER, CANDIDATE, LEADER, CRASHED };

class RaftNode {
private:
    int id;
    int port;
    State state = State::FOLLOWER;
    int currentTerm = 0;
    int votedFor = -1;
    int currentLeaderId = 1;
    bool active = true;

    SOCKET sock = INVALID_SOCKET;
    std::thread listenerThread;
    std::thread timerThread;
    std::mutex stateMtx;

    std::chrono::steady_clock::time_point lastHeartbeatTime;
    int electionTimeoutMs;
    int votesGranted = 0;

    std::map<int, int> clusterMap = {
        {1, 8081},
        {2, 8082},
        {3, 8083},
        {4, 8084},
        {5, 8085}
    };

    void sendUdp(int targetPort, const std::string& data) {
        std::string encrypted = encryptAES(data, AES_KEY, AES_IV);
        sockaddr_in destAddr;
        destAddr.sin_family = AF_INET;
        destAddr.sin_port = htons(targetPort);
        inet_pton(AF_INET, "127.0.0.1", &destAddr.sin_addr);
        sendto(sock, encrypted.c_str(), encrypted.length(), 0, (SOCKADDR*)&destAddr, sizeof(destAddr));
    }

    void broadcast(const std::string& data) {
        for (auto const& [nodeId, nodePort] : clusterMap) {
            if (nodeId != id) {
                sendUdp(nodePort, data);
            }
        }
    }

    int getRandTimeout() {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_int_distribution<> distr(1500, 3000);
        return distr(gen);
    }

    std::string getStateString() {
        switch (state) {
            case State::FOLLOWER: return "FOLLOWER";
            case State::CANDIDATE: return "CANDIDATE";
            case State::LEADER: return "LEADER";
            case State::CRASHED: return "CRASHED";
        }
        return "UNKNOWN";
    }

    void runTimerLoop() {
        while (active) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            std::lock_guard<std::mutex> lock(stateMtx);

            if (state == State::CRASHED) continue;

            if (state == State::LEADER) {
                static auto lastHeartbeatSent = std::chrono::steady_clock::now();
                auto now = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::milliseconds>(now - lastHeartbeatSent).count() >= 200) {
                    raft::AppendEntriesRequest req;
                    req.set_term(currentTerm);
                    req.set_leader_id(id);
                    
                    std::string payload;
                    req.SerializeToString(&payload);
                    broadcast(payload);
                    lastHeartbeatSent = now;
                }
            } 
            else if (state == State::FOLLOWER || state == State::CANDIDATE) {
                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastHeartbeatTime).count();
                
                if (elapsed >= electionTimeoutMs) {
                    state = State::CANDIDATE;
                    currentTerm++;
                    votedFor = id;
                    votesGranted = 1;
                    electionTimeoutMs = getRandTimeout();
                    lastHeartbeatTime = now;

                    std::ostringstream oss;
                    oss << "[Node " << id << " - CANDIDATE] Timeout expired! Initiating Election for Term " << currentTerm;
                    safePrint(oss.str());

                    raft::VoteRequest req;
                    req.set_term(currentTerm);
                    req.set_candidate_id(id);

                    std::string payload;
                    req.SerializeToString(&payload);
                    broadcast(payload);
                }
            }
        }
    }

    void runListenerLoop() {
        char buffer[1024];
        sockaddr_in clientAddr;
        int clientAddrLen = sizeof(clientAddr);

        while (active) {
            DWORD timeout = 200;
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));

            int bytes = recvfrom(sock, buffer, sizeof(buffer) - 1, 0, (SOCKADDR*)&clientAddr, &clientAddrLen);
            if (bytes > 0) {
                std::string encryptedMsg(buffer, bytes);
                std::string msg = decryptAES(encryptedMsg, AES_KEY, AES_IV);
                if (msg.empty()) {
                    continue;
                }

                std::lock_guard<std::mutex> lock(stateMtx);
                if (state == State::CRASHED) continue;

                if (msg == "KILL") {
                    std::ostringstream oss;
                    oss << "\n============================================\n"
                        << "[Node " << id << " - " << getStateString() << "] RECEIVED KILL REQUEST! SIMULATING CRASH...\n"
                        << "============================================";
                    safePrint(oss.str());

                    state = State::CRASHED;
                    
                    raft::TelemetryResponse resp;
                    resp.set_success(false);
                    resp.set_error_message("SERVER_CRASHED");
                    std::string payload;
                    resp.SerializeToString(&payload);
                    std::string encryptedResp = encryptAES(payload, AES_KEY, AES_IV);
                    sendto(sock, encryptedResp.c_str(), encryptedResp.length(), 0, (SOCKADDR*)&clientAddr, clientAddrLen);
                    
                    closesocket(sock);
                    sock = INVALID_SOCKET;
                    active = false;
                    continue;
                }

                raft::AppendEntriesRequest appendReq;
                if (appendReq.ParseFromString(msg)) {
                    if (appendReq.term() >= currentTerm) {
                        currentTerm = appendReq.term();
                        currentLeaderId = appendReq.leader_id();
                        lastHeartbeatTime = std::chrono::steady_clock::now();
                        
                        if (state != State::FOLLOWER) {
                            state = State::FOLLOWER;
                            std::ostringstream oss;
                            oss << "[Node " << id << " - FOLLOWER] Stepped down. Node " << currentLeaderId << " is Leader.";
                            safePrint(oss.str());
                        }

                        raft::AppendEntriesResponse appendResp;
                        appendResp.set_term(currentTerm);
                        appendResp.set_success(true);
                        std::string payload;
                        appendResp.SerializeToString(&payload);
                        sendUdp(clusterMap[currentLeaderId], payload);
                    }
                    continue;
                }

                raft::VoteRequest voteReq;
                if (voteReq.ParseFromString(msg)) {
                    raft::VoteResponse voteResp;
                    
                    if (voteReq.term() > currentTerm) {
                        currentTerm = voteReq.term();
                        state = State::FOLLOWER;
                        votedFor = -1;
                    }
                    
                    voteResp.set_term(currentTerm);

                    if (voteReq.term() == currentTerm && (votedFor == -1 || votedFor == voteReq.candidate_id())) {
                        votedFor = voteReq.candidate_id();
                        voteResp.set_vote_granted(true);
                        lastHeartbeatTime = std::chrono::steady_clock::now();

                        std::ostringstream oss;
                        oss << "[Node " << id << " - FOLLOWER] Voted YES for Node " << voteReq.candidate_id() << " in Term " << currentTerm;
                        safePrint(oss.str());
                    } else {
                        voteResp.set_vote_granted(false);
                    }

                    std::string payload;
                    voteResp.SerializeToString(&payload);
                    sendUdp(clusterMap[voteReq.candidate_id()], payload);
                    continue;
                }

                raft::VoteResponse voteResp;
                if (voteResp.ParseFromString(msg)) {
                    if (state == State::CANDIDATE && voteResp.term() == currentTerm && voteResp.vote_granted()) {
                        votesGranted++;
                        std::ostringstream oss;
                        oss << "[Node " << id << " - CANDIDATE] Got Vote! Current count: " << votesGranted << "/5";
                        safePrint(oss.str());

                        if (votesGranted >= 3) {
                            state = State::LEADER;
                            currentLeaderId = id;
                            std::ostringstream ossL;
                            ossL << "\n============================================\n"
                                 << "[Node " << id << " - LEADER] *** WON ELECTION FOR TERM " << currentTerm << " ***\n"
                                 << "============================================";
                            safePrint(ossL.str());
                        }
                    }
                    continue;
                }

                raft::TelemetryPayload clientPayload;
                if (clientPayload.ParseFromString(msg)) {
                    raft::TelemetryResponse clientResp;

                    if (state == State::LEADER) {
                        std::string expectedHash = calculateSHA256(clientPayload.data_payload());
                        bool integrityMatch = (expectedHash == clientPayload.sha256_hash());

                        std::ostringstream oss;
                        oss << "\n--------------------------------------------\n"
                            << "[Node " << id << " - LEADER] CLIENT TELEMETRY RECEIVED:\n"
                            << "  Timestamp: " << clientPayload.timestamp() << " us\n"
                            << "  Data     : " << clientPayload.data_payload() << "\n"
                            << "  Integrity: SHA-256 " << (integrityMatch ? "MATCH" : "MISMATCH") 
                            << " (" << clientPayload.sha256_hash() << ")\n"
                            << "--------------------------------------------";
                        safePrint(oss.str());

                        if (integrityMatch) {
                            clientResp.set_success(true);
                        } else {
                            clientResp.set_success(false);
                            clientResp.set_error_message("INTEGRITY_CHECK_FAILED");
                        }
                    } else {
                        clientResp.set_success(false);
                        clientResp.set_redirect_leader_ip(std::to_string(currentLeaderId));
                    }

                    std::string payload;
                    clientResp.SerializeToString(&payload);
                    std::string encryptedResp = encryptAES(payload, AES_KEY, AES_IV);
                    sendto(sock, encryptedResp.c_str(), encryptedResp.length(), 0, (SOCKADDR*)&clientAddr, clientAddrLen);
                    continue;
                }
            }
        }
    }

public:
    RaftNode(int nodeId) : id(nodeId) {
        port = clusterMap[nodeId];
        electionTimeoutMs = getRandTimeout();
        lastHeartbeatTime = std::chrono::steady_clock::now();

        WSADATA wsaData;
        WSAStartup(MAKEWORD(2, 2), &wsaData);

        sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        sockaddr_in serverAddr;
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(port);
        serverAddr.sin_addr.s_addr = INADDR_ANY;

        int bindResult = bind(sock, (SOCKADDR*)&serverAddr, sizeof(serverAddr));
        if (bindResult != 0) {
            std::ostringstream oss;
            oss << "[Node " << id << "] FAILED TO BIND TO PORT " << port << ". Socket error: " << WSAGetLastError();
            safePrint(oss.str());
            active = false;
            return;
        }

        if (id == 1) {
            state = State::LEADER;
            currentLeaderId = 1;
            std::ostringstream oss;
            oss << "[Node 1 - LEADER] Cluster Initialized. Active on Port " << port;
            safePrint(oss.str());
        } else {
            state = State::FOLLOWER;
            std::ostringstream oss;
            oss << "[Node " << id << " - FOLLOWER] Active on Port " << port << " | Election Timeout: " << electionTimeoutMs << "ms";
            safePrint(oss.str());
        }

        listenerThread = std::thread(&RaftNode::runListenerLoop, this);
        timerThread = std::thread(&RaftNode::runTimerLoop, this);
    }

    ~RaftNode() {
        active = false;
        if (sock != INVALID_SOCKET) {
            closesocket(sock);
        }
        if (listenerThread.joinable()) listenerThread.join();
        if (timerThread.joinable()) timerThread.join();
        WSACleanup();
    }
};

int main(int argc, char* argv[]) {
    if (argc > 1) {
        int nodeId = std::stoi(argv[1]);
        if (nodeId < 1 || nodeId > 5) {
            std::cerr << "Invalid Node ID. Must be between 1 and 5.\n";
            return 1;
        }

        std::cout << "Starting Raft Node " << nodeId << " as a standalone process...\n";
        RaftNode node(nodeId);
        std::cout << "Press Enter to shut down this node...\n";
        std::cin.get();
        return 0;
    }

    std::cout << "=========================================================\n";
    std::cout << "Starting Local Raft Cluster Simulation (5 Concurrent Nodes)\n";
    std::cout << "=========================================================\n";

    std::vector<std::unique_ptr<RaftNode>> cluster;
    for (int i = 1; i <= 5; ++i) {
        cluster.push_back(std::make_unique<RaftNode>(i));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "\nAll 5 nodes are online and running real UDP sockets on ports 8081-8085!\n";
    std::cout << "Send data to Port 8081 using client.exe.\n";
    std::cout << "Press Enter at any time to shut down the entire cluster...\n\n";
    std::cin.get();

    std::cout << "Shutting down cluster...\n";
    cluster.clear();
    return 0;
}
