// Minimal C++ sensor engine stub - outputs a JSON line per sample to stdout.
// This file is a placeholder showing the intended approach; not built in this prototype.

#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>
#include <csignal>
#include <random>
#include <sstream>
#include <vector>
#include <mutex>
#include <algorithm>

#ifdef _WIN32
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  pragma comment(lib, "Ws2_32.lib")
#endif

static std::atomic<bool> g_running{true};
static std::atomic<bool> g_tcp_enabled{false};
static int g_tcp_port = 20123;

void handle_signal(int) {
    g_running = false;
}

#ifdef _WIN32
struct TcpServer {
    SOCKET listenSock = INVALID_SOCKET;
    std::vector<SOCKET> clients;
    std::mutex mtx;

    bool init(int port) {
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2,2), &wsaData) != 0) return false;
        listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (listenSock == INVALID_SOCKET) return false;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons((u_short)port);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        int opt = 1; setsockopt(listenSock, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
        if (bind(listenSock, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) return false;
        if (listen(listenSock, 4) == SOCKET_ERROR) return false;
        return true;
    }
    void accept_loop() {
        while (g_running.load()) {
            SOCKET s = accept(listenSock, nullptr, nullptr);
            if (s == INVALID_SOCKET) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            std::lock_guard<std::mutex> lk(mtx);
            clients.push_back(s);
        }
    }
    void broadcast(const std::string& line) {
        std::lock_guard<std::mutex> lk(mtx);
        for (size_t i=0; i<clients.size();) {
            SOCKET s = clients[i];
            int sent = send(s, line.c_str(), (int)line.size(), 0);
            if (sent == SOCKET_ERROR) {
                closesocket(s);
                clients.erase(clients.begin()+i);
            } else {
                ++i;
            }
        }
    }
    void shutdown_all() {
        std::lock_guard<std::mutex> lk(mtx);
        for (auto s: clients) closesocket(s);
        clients.clear();
        if (listenSock != INVALID_SOCKET) closesocket(listenSock);
        WSACleanup();
    }
};
#endif

int main(int argc, char** argv) {
    int interval_ms = 800;
    if (argc > 1) {
        try { interval_ms = (std::max)(50, std::stoi(argv[1])); } catch (...) {}
    }
    for (int i=1;i<argc;i++) {
    for (int i=1;i<argc;i++) {
        std::string a = argv[i];
        if (a == "-t" || a == "--tcp") { g_tcp_enabled = true; if (i+1<argc) { try { g_tcp_port = std::stoi(argv[i+1]); i++; } catch (...) {} } }
    }

    std::signal(SIGINT, handle_signal);
#ifdef SIGTERM
    std::signal(SIGTERM, handle_signal);
#endif

    std::mt19937 rng{std::random_device{}()};
    std::uniform_real_distribution<double> cpu_d(1.0, 50.0), ram_d(20.0, 80.0), gpu_d(0.0, 40.0), temp_d(35.0, 85.0);

#ifdef _WIN32
    TcpServer server;
    std::thread accept_thread;
    if (g_tcp_enabled.load()) {
        if (server.init(g_tcp_port)) {
            accept_thread = std::thread([&]{ server.accept_loop(); });
        } else {
            std::cerr << "TCP init failed on port " << g_tcp_port << "\n";
        }
    }
#endif

    while (g_running.load()) {
        double cpu = cpu_d(rng);
        double ram = ram_d(rng);
        double gpu = gpu_d(rng);
        double temp = temp_d(rng);

        std::ostringstream oss;
        oss.setf(std::ios::fixed); oss.precision(1);
        oss << "{\"cpu_percent\": " << cpu
            << ", \"ram_percent\": " << ram
            << ", \"gpu_percent\": " << gpu
            << ", \"temps\": {\"cpu_package\": " << temp << "}}";
        const std::string line = oss.str() + "\n";
        std::cout << line;
        std::cout.flush();

#ifdef _WIN32
        if (g_tcp_enabled.load()) {
            server.broadcast(line);
        }
#endif
        std::cout.flush();
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
#ifdef _WIN32
    if (accept_thread.joinable()) accept_thread.join();
    server.shutdown_all();
#endif
    return 0;
}
