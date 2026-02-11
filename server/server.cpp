#include <iostream>
#include <thread>
#include <vector>
#include <map>
#include <mutex>
#include <atomic>
#include <csignal>
#include <cstring>
#include <ctime>
#include <cerrno>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "../common/protocol.h"

#define SERVER_PORT 5341
#define MAX_CLIENT_QUEUE 20

/**
 * @brief 服务端维护的客户端信息表项
 * - id：服务端分配的客户端编号
 * - sockfd：与该客户端对应的 TCP 连接句柄
 * - ip：客户端 IPv4 地址
 * - port：客户端源端口
 */
struct ClientInfo {
    uint32_t id;
    int sockfd;
    uint32_t ip;
    uint16_t port;
};

/** @brief 全局客户端表 */
std::map<uint32_t, ClientInfo> g_clients;
/** @brief 保护 g_clients 的互斥锁 */
std::mutex g_clients_mutex;
/** @brief 全局运行标记 */
std::atomic<bool> g_running(true);
/** @brief 监听 socket */
int g_listen_sock = -1;
/** @brief 分配客户端 id 的自增计数器 */
std::atomic<uint32_t> g_next_client_id(1);

/**
 * @brief 信号处理，触发服务端退出
 */
void signal_handler(int ) {
    g_running.store(false);
    // 关闭监听 socket，让 accept 返回
    if (g_listen_sock >= 0) {
        close(g_listen_sock);
        g_listen_sock = -1;
    }
}

/**
 * @brief 构造“获取时间”的响应包
 * payload 格式：
 * [0] result
 * [1] reserved
 * [2..] time string
 * @param req 原请求包
 * @return 响应 Packet
 */
Packet make_time_response(const Packet& req) {
    Packet res;
    res.msg_type = MSG_TYPE_RESPONSE;
    res.code     = CODE_TIME;
    res.seq      = req.seq;

    std::vector<uint8_t> payload;
    payload.push_back(0); // result = 0 成功
    payload.push_back(0); // reserved

    // 取本地时间并格式化
    std::time_t t = std::time(nullptr);
    std::tm* tm_info = std::localtime(&t);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm_info);
    std::string time_str(buf);

    payload.insert(payload.end(), time_str.begin(), time_str.end());
    res.payload = std::move(payload);
    return res;
}

/**
 * @brief 构造“获取 hostname”的响应包
 * payload 格式：
 * [0] result
 * [1] reserved
 * [2..] hostname string
 * @param req 原请求包
 * @return 响应 Packet
 */
Packet make_name_response(const Packet& req) {
    Packet res;
    res.msg_type = MSG_TYPE_RESPONSE;
    res.code     = CODE_NAME;
    res.seq      = req.seq;

    std::vector<uint8_t> payload;
    payload.push_back(0);
    payload.push_back(0);

    char hostname[256] = {0};
    if (gethostname(hostname, sizeof(hostname) - 1) != 0) {
        std::strncpy(hostname, "unknown", sizeof(hostname) - 1);
    }
    std::string name(hostname);
    payload.insert(payload.end(), name.begin(), name.end());
    res.payload = std::move(payload);
    return res;
}

/**
 * @brief 构造“获取客户端列表”的响应包
 * payload 格式：
 * [0] result
 * [1] reserved
 * [2..3] count(uint16)
 * @param req 原请求包
 * @return 客户端列表响应 Packet
 */
Packet make_list_response(const Packet& req) {
    Packet res;
    res.msg_type = MSG_TYPE_RESPONSE;
    res.code     = CODE_LIST;
    res.seq      = req.seq;

    std::vector<uint8_t> payload;
    payload.push_back(0);
    payload.push_back(0);

    // 拷贝，减少持锁时间
    std::vector<ClientInfo> snapshot;
    {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        snapshot.reserve(g_clients.size());
        for (auto& kv : g_clients) {
            snapshot.push_back(kv.second);
        }
    }

    // 写入客户端数量 count
    uint8_t buf16[2];
    uint16_t count = static_cast<uint16_t>(snapshot.size());
    write_u16(buf16, count);
    payload.insert(payload.end(), buf16, buf16 + 2);

    // 逐条写入客户端记录，id(4) + ip(4) + port(2)
    for (const auto& c : snapshot) {
        uint8_t b4[4], b2[2];
        write_u32(b4, c.id);
        payload.insert(payload.end(), b4, b4 + 4);
        write_u32(b4, c.ip);
        payload.insert(payload.end(), b4, b4 + 4);
        write_u16(b2, c.port);
        payload.insert(payload.end(), b2, b2 + 2);
    }

    res.payload = std::move(payload);
    return res;
}

/**
 * @brief 构造错误响应包
 * payload 格式：
 * [0] result=1
 * [1] reserved
 * [2..] error message string
 * @param req 原请求包
 * @param code 响应使用的 code
 * @param msg 错误描述字符串
 */
Packet make_error_response(const Packet& req, uint16_t code, const std::string& msg) {
    Packet res;
    res.msg_type = MSG_TYPE_RESPONSE;
    res.code     = code;
    res.seq      = req.seq;

    std::vector<uint8_t> payload;
    payload.push_back(1); // result = 1 失败
    payload.push_back(0);
    payload.insert(payload.end(), msg.begin(), msg.end());
    res.payload = std::move(payload);
    return res;
}

/**
 * @brief 处理“发送消息”的请求
 * 请求 payload 格式：
 * [0..3] target_id(uint32)
 * [4..]  text string
 * @param req 原请求包
 * @param from_client_id 源客户端 id
 * @return 对源客户端的响应 Packet
 */
Packet handle_message_request(const Packet& req, uint32_t from_client_id) {
    if (req.payload.size() < 4) {
        return make_error_response(req, CODE_MESSAGE, "Bad MESSAGE payload");
    }

    // 解析目标客户端 ID 与文本内容
    uint32_t target_id = read_u32(req.payload.data());
    std::string text(req.payload.begin() + 4, req.payload.end());

    int target_dup = -1;
    {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        auto it = g_clients.find(target_id);
        if (it != g_clients.end()) {
            // dup 一个 fd，防止线程 close 后 fd 被复用
            target_dup = dup(it->second.sockfd);
        }
    }

    if (target_dup < 0) {
        return make_error_response(req, CODE_MESSAGE, "Target client not found");
    }

    // 构造指示消息
    Packet ind;
    ind.msg_type = MSG_TYPE_INDICATION;
    ind.code     = CODE_IND_MESSAGE;
    ind.seq      = 0;

    std::vector<uint8_t> payload;
    uint8_t b4[4];
    write_u32(b4, from_client_id);
    payload.insert(payload.end(), b4, b4 + 4);
    payload.insert(payload.end(), text.begin(), text.end());
    ind.payload = std::move(payload);

    // 发送给目标客户端
    bool ok = send_packet(target_dup, ind);
    close(target_dup);

    if (!ok) {
        return make_error_response(req, CODE_MESSAGE, "Send to target failed");
    }

    // 返回源客户端转发成功响应
    Packet res;
    res.msg_type = MSG_TYPE_RESPONSE;
    res.code     = CODE_MESSAGE;
    res.seq      = req.seq;

    std::vector<uint8_t> res_payload;
    res_payload.push_back(0);
    res_payload.push_back(0);
    std::string ok_msg = "Message delivered";
    res_payload.insert(res_payload.end(), ok_msg.begin(), ok_msg.end());
    res.payload = std::move(res_payload);
    return res;
}

/**
 * @brief 客户端处理线程
 * 线程职责：
 * - 循环 recv_packet() 读取完整请求包
 * - 根据 req.code 分发到对应处理函数
 * - send_packet() 发送响应包
 * - 处理断开请求、连接异常关闭等情况
 * @param client_id 服务端分配的客户端 id
 * @param sockfd 与该客户端对应的 socket
 */
void client_thread(uint32_t client_id, int sockfd) {
    std::cout << "[Info] Client " << client_id << " handler started.\n";
    
    std::vector<uint8_t> buffer;  // 粘包/半包处理
    Packet req;

    while (g_running.load()) {
        // 接收请求包
        if (!recv_packet(sockfd, req, buffer)) {
            std::cout << "[Info] Client " << client_id << " disconnected.\n";
            break;
        }
        if (req.msg_type != MSG_TYPE_REQUEST) {
            std::cout << "[Warn] Non-request packet from client " << client_id << ".\n";
            continue;
        }

        Packet res;
        bool need_send = true;

        // 按功能码分发处理
        switch (req.code) {
            case CODE_TIME:
                res = make_time_response(req);
                break;
            case CODE_NAME:
                res = make_name_response(req);
                break;
            case CODE_LIST:
                res = make_list_response(req);
                break;
            case CODE_MESSAGE:
                res = handle_message_request(req, client_id);
                break;
            case CODE_DISCONNECT:
                std::cout << "[Info] Client " << client_id << " requested disconnect.\n";
                need_send = false;
                goto end_loop;
            default:
                res = make_error_response(req, req.code, "Unknown request code");
                break;
        }

        // 发送响应包
        if (need_send) {
            if (!send_packet(sockfd, res)) {
                std::cout << "[Error] Failed to send response to client " << client_id << ".\n";
                break;
            }
        }
    }

end_loop:
    // 清理客户端表项
    {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        g_clients.erase(client_id);
    }
    close(sockfd);
    std::cout << "[Info] Client " << client_id << " handler exiting.\n";
}

int main() {
    // 注册信号
    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);

    // 创建监听 socket
    g_listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (g_listen_sock < 0) {
        std::cerr << "[Error] Failed to create socket\n";
        return -1;
    }

    // DEBUG：避免服务端重启时端口处于 TIME_WAIT 导致 bind 失败
    int opt = 1;
    setsockopt(g_listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // bind 到本地端口
    struct sockaddr_in server_addr{};
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family      = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port        = htons(SERVER_PORT);
    if (bind(g_listen_sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "[Error] Bind failed\n";
        close(g_listen_sock);
        return -1;
    }

    // 开启连接监听队列
    if (listen(g_listen_sock, MAX_CLIENT_QUEUE) < 0) {
        std::cerr << "[Error] Listen failed\n";
        close(g_listen_sock);
        return -1;
    }

    std::cout << "[Info] Server listening on port " << SERVER_PORT << "...\n";

    // 保存客户端线程对象
    std::vector<std::thread> threads;

    // 主线程循环 accept 新连接
    while (g_running.load()) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_sock = accept(g_listen_sock, (struct sockaddr*)&client_addr, &client_len);
        if (client_sock < 0) {
            // g_running=false 说明监听 socket 被关闭导致 accept 返回，退出
            if (!g_running.load()) {
                break;
            }
            // 被信号中断，继续
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "[Error] Accept failed\n";
            continue;
        }

        // 为新客户端分配 id
        uint32_t client_id = g_next_client_id++;

        // 记录客户端 ip/port
        uint32_t ip   = ntohl(client_addr.sin_addr.s_addr);
        uint16_t port = ntohs(client_addr.sin_port);

        // 写入全局客户端表
        {
            std::lock_guard<std::mutex> lock(g_clients_mutex);
            g_clients[client_id] = ClientInfo{client_id, client_sock, ip, port};
        }

        // 打印连接信息
        char ip_str[INET_ADDRSTRLEN];
        struct in_addr addr{};
        addr.s_addr = htonl(ip);
        inet_ntop(AF_INET, &addr, ip_str, sizeof(ip_str));

        std::cout << "[Info] New client " << client_id
                  << " from " << ip_str << ":" << port << "\n";

        // 创建子线程处理该客户端
        threads.emplace_back(client_thread, client_id, client_sock);
    }

    // 退出清理
    {
        std::vector<int> fds;
        {
            std::lock_guard<std::mutex> lock(g_clients_mutex);
            fds.reserve(g_clients.size());
            for (auto& kv : g_clients) {
                fds.push_back(kv.second.sockfd);
            }
        }
        for (int fd : fds) {
            shutdown(fd, SHUT_RDWR);
        }
    }

    // 关闭监听 socket
    if (g_listen_sock >= 0) {
        close(g_listen_sock);
        g_listen_sock = -1;
    }

    // join 客户端线程
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    std::cout << "[Info] Server stopped.\n";
    return 0;
}
