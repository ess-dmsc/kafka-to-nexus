#pragma once

#include <cstdint>
#include <thread>
#include <atomic>

// warning global variables
std::atomic<uint64_t> GlobalState;
std::atomic<uint64_t> GlobalWritesDone;


const char * tcp_port = "3490";
const int Backlog{4};

class StatusServer {
public:
    StatusServer();
    ~StatusServer();
    
    void Start();

private:
    std::thread Thread;
    bool StopThread{false};
    void ThreadMain();

};
