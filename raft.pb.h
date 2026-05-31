#pragma once
#include <string>
#include <sstream>
#include <vector>
#include <cstdint>

// ==========================================
// PROTOBUF GENERATED HEADER MOCK (raft.pb.h)
// ==========================================

namespace raft {

// Helper to split a string by delimiter
inline std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> result;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        result.push_back(item);
    }
    return result;
}

// ------------------------------------------
// TelemetryPayload (Client -> Server)
// ------------------------------------------
class TelemetryPayload {
private:
    int64_t timestamp_ = 0;
    std::string data_payload_;
    std::string sha256_hash_;

public:
    int64_t timestamp() const { return timestamp_; }
    void set_timestamp(int64_t val) { timestamp_ = val; }
    std::string data_payload() const { return data_payload_; }
    void set_data_payload(const std::string& val) { data_payload_ = val; }
    std::string sha256_hash() const { return sha256_hash_; }
    void set_sha256_hash(const std::string& val) { sha256_hash_ = val; }

    bool SerializeToString(std::string* output) const {
        std::ostringstream oss;
        oss << "TREQ:" << timestamp_ << "|" << data_payload_ << "|" << sha256_hash_;
        *output = oss.str();
        return true;
    }

    bool ParseFromString(const std::string& input) {
        if (input.rfind("TREQ:", 0) != 0) return false; // Must start with TREQ:
        std::string raw = input.substr(5);
        auto parts = split(raw, '|');
        if (parts.size() >= 2) {
            try { timestamp_ = std::stoll(parts[0]); } catch(...) { timestamp_ = 0; }
            data_payload_ = parts[1];
            sha256_hash_ = (parts.size() >= 3) ? parts[2] : "";
            return true;
        }
        return false;
    }
};

// ------------------------------------------
// TelemetryResponse (Server -> Client)
// ------------------------------------------
class TelemetryResponse {
private:
    bool success_ = false;
    std::string redirect_leader_ip_;
    std::string error_message_;

public:
    bool success() const { return success_; }
    void set_success(bool val) { success_ = val; }
    std::string redirect_leader_ip() const { return redirect_leader_ip_; }
    void set_redirect_leader_ip(const std::string& val) { redirect_leader_ip_ = val; }
    std::string error_message() const { return error_message_; }
    void set_error_message(const std::string& val) { error_message_ = val; }

    bool SerializeToString(std::string* output) const {
        std::ostringstream oss;
        oss << "TRES:" << (success_ ? "1" : "0") << "|" << redirect_leader_ip_ << "|" << error_message_;
        *output = oss.str();
        return true;
    }

    bool ParseFromString(const std::string& input) {
        if (input.rfind("TRES:", 0) != 0) return false;
        std::string raw = input.substr(5);
        auto parts = split(raw, '|');
        if (parts.size() >= 2) {
            success_ = (parts[0] == "1");
            redirect_leader_ip_ = parts[1];
            error_message_ = (parts.size() >= 3) ? parts[2] : "";
            return true;
        }
        return false;
    }
};

// ------------------------------------------
// VoteRequest (Candidate -> Followers)
// ------------------------------------------
class VoteRequest {
private:
    int32_t term_ = 0;
    int32_t candidate_id_ = 0;

public:
    int32_t term() const { return term_; }
    void set_term(int32_t val) { term_ = val; }
    int32_t candidate_id() const { return candidate_id_; }
    void set_candidate_id(int32_t val) { candidate_id_ = val; }

    bool SerializeToString(std::string* output) const {
        std::ostringstream oss;
        oss << "VREQ:" << term_ << "|" << candidate_id_;
        *output = oss.str();
        return true;
    }

    bool ParseFromString(const std::string& input) {
        if (input.rfind("VREQ:", 0) != 0) return false;
        std::string raw = input.substr(5);
        auto parts = split(raw, '|');
        if (parts.size() >= 2) {
            term_ = std::stoi(parts[0]);
            candidate_id_ = std::stoi(parts[1]);
            return true;
        }
        return false;
    }
};

// ------------------------------------------
// VoteResponse (Follower -> Candidate)
// ------------------------------------------
class VoteResponse {
private:
    int32_t term_ = 0;
    bool vote_granted_ = false;

public:
    int32_t term() const { return term_; }
    void set_term(int32_t val) { term_ = val; }
    bool vote_granted() const { return vote_granted_; }
    void set_vote_granted(bool val) { vote_granted_ = val; }

    bool SerializeToString(std::string* output) const {
        std::ostringstream oss;
        oss << "VRES:" << term_ << "|" << (vote_granted_ ? "1" : "0");
        *output = oss.str();
        return true;
    }

    bool ParseFromString(const std::string& input) {
        if (input.rfind("VRES:", 0) != 0) return false;
        std::string raw = input.substr(5);
        auto parts = split(raw, '|');
        if (parts.size() >= 2) {
            term_ = std::stoi(parts[0]);
            vote_granted_ = (parts[1] == "1");
            return true;
        }
        return false;
    }
};

// ------------------------------------------
// AppendEntriesRequest (Leader -> Followers - Heartbeat / Replication)
// ------------------------------------------
class AppendEntriesRequest {
private:
    int32_t term_ = 0;
    int32_t leader_id_ = 0;

public:
    int32_t term() const { return term_; }
    void set_term(int32_t val) { term_ = val; }
    int32_t leader_id() const { return leader_id_; }
    void set_leader_id(int32_t val) { leader_id_ = val; }

    bool SerializeToString(std::string* output) const {
        std::ostringstream oss;
        oss << "AREQ:" << term_ << "|" << leader_id_;
        *output = oss.str();
        return true;
    }

    bool ParseFromString(const std::string& input) {
        if (input.rfind("AREQ:", 0) != 0) return false;
        std::string raw = input.substr(5);
        auto parts = split(raw, '|');
        if (parts.size() >= 2) {
            term_ = std::stoi(parts[0]);
            leader_id_ = std::stoi(parts[1]);
            return true;
        }
        return false;
    }
};

// ------------------------------------------
// AppendEntriesResponse (Follower -> Leader)
// ------------------------------------------
class AppendEntriesResponse {
private:
    int32_t term_ = 0;
    bool success_ = false;

public:
    int32_t term() const { return term_; }
    void set_term(int32_t val) { term_ = val; }
    bool success() const { return success_; }
    void set_success(bool val) { success_ = val; }

    bool SerializeToString(std::string* output) const {
        std::ostringstream oss;
        oss << "ARES:" << term_ << "|" << (success_ ? "1" : "0");
        *output = oss.str();
        return true;
    }

    bool ParseFromString(const std::string& input) {
        if (input.rfind("ARES:", 0) != 0) return false;
        std::string raw = input.substr(5);
        auto parts = split(raw, '|');
        if (parts.size() >= 2) {
            term_ = std::stoi(parts[0]);
            success_ = (parts[1] == "1");
            return true;
        }
        return false;
    }
};

} // namespace raft
