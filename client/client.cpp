#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <vector>
#include <cstring>
#include <cerrno>
#include <chrono>

#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "../common/protocol.h"

#define DEFAULT_SERVER_PORT 5341

/** @brief 客户端运行状态 */
std::atomic<bool> g_running(true);
/** @brief 当前服务器连接状态 */
std::atomic<bool> g_connected(false);
/** @brief TCP socket fd */
std::atomic<int> g_sockfd(-1);
/**
 * @brief 区分是否为主动断开连接
 */
std::atomic<bool> g_manual_disconnect(false);
/** @brief 消息队列 */
std::queue<Packet> g_msg_queue;
std::mutex g_queue_mutex;
std::condition_variable g_queue_cv;
/** @brief 请求序列号自增 */
std::atomic<uint32_t> g_next_seq(1);
/** @brief 接收线程对象 */
std::thread g_recv_thread;
/** @brief 输出锁，控制输出顺序 */
std::mutex g_io_mutex;
/** @brief 等待机制的互斥锁 */
std::mutex g_wait_mutex;
/** @brief 等待机制条件变量 */
std::condition_variable g_wait_cv;
/** @brief 主线程等待状态 */
bool g_waiting = false;
/** @brief 当前等待的响应 seq */
uint32_t g_wait_seq = 0;
/** @brief 当前等待的响应 code */
uint16_t g_wait_code = 0;

/**
 * @brief 打印菜单
 */
void print_menu_locked() {
    std::cout << "========== Client Menu ==========\n";
    if (!g_connected.load()) {
        std::cout << "1. 连接服务器\n";
        std::cout << "7. 退出\n";
    } else {
        std::cout << "1. 禁用，服务器已连接\n";
        std::cout << "2. 断开连接\n";
        std::cout << "3. 获取时间\n";
        std::cout << "4. 获取 hostname\n";
        std::cout << "5. 获取客户端列表\n";
        std::cout << "6. 发送消息\n";
        std::cout << "7. 退出\n";
    }
    std::cout << "=================================\n";
    std::cout << "请输入选项：";
    std::cout.flush();
}

/**
 * @brief 加锁的菜单打印
 */
void print_menu() {
    std::lock_guard<std::mutex> lk(g_io_mutex);
    std::cout << "\n";
    print_menu_locked();
}

/**
 * @brief 解析 int
 * @param s 输入字符串
 * @param out 输出：解析结果
 * @return true 成功解析且整串都是数字；false 解析失败
 */
static bool parse_int(const std::string& s, int& out) {
    try {
        size_t idx = 0;
        int v = std::stoi(s, &idx);
        if (idx != s.size()) return false;
        out = v;
        return true;
    } catch (...) {
        return false;
    }
}

/**
 * @brief 解析 uint16，用于端口
 * @param s 输入字符串
 * @param out 输出：端口
 * @return true 成功且范围合法；false 失败
 */
static bool parse_u16(const std::string& s, uint16_t& out) {
    try {
        size_t idx = 0;
        unsigned long v = std::stoul(s, &idx);
        if (idx != s.size() || v > 65535UL) return false;
        out = static_cast<uint16_t>(v);
        return true;
    } catch (...) {
        return false;
    }
}

/**
 * @brief 解析 uint32，用于客户端 id
 * @param s 输入字符串
 * @param out 输出：id
 * @return true 成功且范围合法；false 失败
 */
static bool parse_u32(const std::string& s, uint32_t& out) {
    try {
        size_t idx = 0;
        unsigned long v = std::stoul(s, &idx);
        if (idx != s.size() || v > 0xFFFFFFFFUL) return false;
        out = static_cast<uint32_t>(v);
        return true;
    } catch (...) {
        return false;
    }
}

/**
 * @brief 安全关闭 socket
 */
static void close_socket_if_open() {
    int s = g_sockfd.exchange(-1);  // 让后续线程看到 socket 已无效
    if (s >= 0) {
        shutdown(s, SHUT_RDWR);  // 唤醒阻塞的 recv()
        close(s);  // 释放 fd
    }
}

/**
 * @brief 停止接收线程
 */
static void stop_recv_thread() {
    g_manual_disconnect.store(true);  // 断开连接
    g_connected.store(false);
    close_socket_if_open();  // 让接收线程循环退出
    if (g_recv_thread.joinable()) {
        g_recv_thread.join();  // join 回收线程
    }
    g_manual_disconnect.store(false);
}

/**
 * @brief 发送请求包，并返回本次请求的 seq
 * @param code 业务功能码
 * @param payload 请求负载
 * @param out_seq 输出：本次请求使用的序列号
 * @return true 发送成功；false 未连接/发送失败
 */
bool send_request(uint16_t code, const std::vector<uint8_t>& payload, uint32_t& out_seq) {
    if (!g_connected.load()) {
        std::lock_guard<std::mutex> lk(g_io_mutex);
        std::cout << "[Warn] 未连接服务器.\n";
        return false;
    }

    int sock = g_sockfd.load();
    if (sock < 0) {
        std::lock_guard<std::mutex> lk(g_io_mutex);
        std::cout << "[Warn] Socket 无效.\n";
        return false;
    }

    // 组装 Packet
    Packet pkt;
    pkt.msg_type = MSG_TYPE_REQUEST;
    pkt.code     = code;
    pkt.seq      = g_next_seq++;
    pkt.payload  = payload;

    out_seq = pkt.seq;

    // 发送
    if (!send_packet(sock, pkt)) {
        std::lock_guard<std::mutex> lk(g_io_mutex);
        std::cout << "[Error] 发送请求失败.\n";
        return false;
    }
    return true;
}

/**
 * @brief 主线程等待某个响应被展示线程打印完
 * @param expect_code 期望响应的功能码
 * @param expect_seq  期望响应的序列号
 * @param timeout_ms  最大等待时间
 */
static void wait_for_response(uint16_t expect_code, uint32_t expect_seq, int timeout_ms = 3000) {
    std::unique_lock<std::mutex> lk(g_wait_mutex);
    // 设置等待目标
    g_waiting = true;
    g_wait_code = expect_code;
    g_wait_seq  = expect_seq;
    // 等待展示线程处理完对应响应或退出/断连
    g_wait_cv.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
        return !g_running.load() || !g_connected.load() || !g_waiting;
    });
    // 超时
    if (g_waiting && g_running.load() && g_connected.load()) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "\n[Warn] 等待响应超时\n";
        g_waiting = false;
    }
}

/**
 * @brief 处理时间响应（CODE_TIME）
 * payload 格式：
 * [0] result (0成功)
 * [1] reserved
 * [2..] 时间字符串
 */
void handle_time_response(const Packet& pkt) {
    if (pkt.payload.size() < 2) return;
    uint8_t result = pkt.payload[0];
    if (result != 0) {
        std::string msg(pkt.payload.begin() + 2, pkt.payload.end());
        std::cout << "[Error] 获取时间失败: " << msg << "\n";
        return;
    }
    std::string time_str(pkt.payload.begin() + 2, pkt.payload.end());
    std::cout << "[Info] 服务器时间: " << time_str << "\n";
}

/**
 * @brief 处理名字响应（CODE_NAME）
 * payload 格式：
 * [0] result
 * [1] reserved
 * [2..] hostname
 */
void handle_name_response(const Packet& pkt) {
    if (pkt.payload.size() < 2) return;
    uint8_t result = pkt.payload[0];
    if (result != 0) {
        std::string msg(pkt.payload.begin() + 2, pkt.payload.end());
        std::cout << "[Error] 获取名字失败: " << msg << "\n";
        return;
    }
    std::string name(pkt.payload.begin() + 2, pkt.payload.end());
    std::cout << "[Info] 服务器名字: " << name << "\n";
}

/**
 * @brief 处理列表响应（CODE_LIST）
 * payload 格式：
 * [0] result
 * [1] reserved
 * [2..3] count(uint16)
 * 后续 count 条记录：id(4)+ip(4)+port(2)
 */
void handle_list_response(const Packet& pkt) {
    if (pkt.payload.size() < 4) return;
    uint8_t result = pkt.payload[0];
    if (result != 0) {
        std::string msg(pkt.payload.begin() + 2, pkt.payload.end());
        std::cout << "[Error] 获取客户端列表失败: " << msg << "\n";
        return;
    }

    uint16_t count = read_u16(pkt.payload.data() + 2);
    size_t offset = 4;

    std::cout << "[Info] 当前客户端数量: " << count << "\n";
    std::cout << "ID\tIP\t\tPort\n";

    // 逐条解析客户端记录并打印
    for (uint16_t i = 0; i < count; ++i) {
        if (offset + 10 > pkt.payload.size()) break;

        uint32_t id   = read_u32(pkt.payload.data() + offset); offset += 4;
        uint32_t ip_h = read_u32(pkt.payload.data() + offset); offset += 4;
        uint16_t port = read_u16(pkt.payload.data() + offset); offset += 2;

        // ip_h 是主机序，要转回网络序给 inet_ntop
        struct in_addr addr{};
        addr.s_addr = htonl(ip_h);
        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr, ip_str, sizeof(ip_str));

        std::cout << id << "\t" << ip_str << "\t" << port << "\n";
    }
}

/**
 * @brief 处理消息发送结果响应（CODE_MESSAGE）
 * payload 格式：
 * [0] result
 * [1] reserved
 * [2..] 结果描述字符串
 */
void handle_message_response(const Packet& pkt) {
    if (pkt.payload.size() < 2) return;
    uint8_t result = pkt.payload[0];
    std::string msg(pkt.payload.begin() + 2, pkt.payload.end());
    if (result == 0) {
        std::cout << "[Info] 发送消息成功: " << msg << "\n";
    } else {
        std::cout << "[Error] 发送消息失败: " << msg << "\n";
    }
}

/**
 * @brief 处理服务器指示消息（CODE_IND_MESSAGE）
 * payload 格式：
 * [0..3] from_id(uint32)
 * [4..]  文本字符串
 */
void handle_ind_message(const Packet& pkt) {
    if (pkt.payload.size() < 4) return;
    uint32_t from_id = read_u32(pkt.payload.data());
    std::string text(pkt.payload.begin() + 4, pkt.payload.end());
    std::cout << "[Msg] 来自客户端 " << from_id << " 的消息: " << text << "\n";
}

/**
 * @brief 展示线程函数
 */
void display_thread_func() {
    while (g_running.load()) {
        Packet pkt;

        // 等待队列里出现新消息
        {
            std::unique_lock<std::mutex> lock(g_queue_mutex);
            g_queue_cv.wait(lock, [] {
                return !g_running.load() || !g_msg_queue.empty();
            });

            // 程序退出且队列空，结束线程
            if (!g_running.load() && g_msg_queue.empty()) {
                break;
            }

            // 取队首消息
            pkt = std::move(g_msg_queue.front());
            g_msg_queue.pop();
        }

        // 根据消息类型进行打印
        if (pkt.msg_type == MSG_TYPE_RESPONSE) {
            {
                std::lock_guard<std::mutex> iolk(g_io_mutex);
                std::cout << "\n";
                switch (pkt.code) {
                    case CODE_TIME:
                        handle_time_response(pkt);
                        break;
                    case CODE_NAME:
                        handle_name_response(pkt);
                        break;
                    case CODE_LIST:
                        handle_list_response(pkt);
                        break;
                    case CODE_MESSAGE:
                        handle_message_response(pkt);
                        break;
                    default:
                        std::cout << "[Warn] 未知响应 code=" << pkt.code << "\n";
                        break;
                }
                std::cout.flush();
            }

            // 若响应是主线程等待目标，解除等待
            {
                std::lock_guard<std::mutex> lk(g_wait_mutex);
                if (g_waiting && pkt.code == g_wait_code && pkt.seq == g_wait_seq) {
                    g_waiting = false;
                }
            }
            g_wait_cv.notify_all();
        } else if (pkt.msg_type == MSG_TYPE_INDICATION) {
            {
                std::lock_guard<std::mutex> iolk(g_io_mutex);
                std::cout << "\n";
                if (pkt.code == CODE_IND_MESSAGE) {
                    handle_ind_message(pkt);
                } else {
                    std::cout << "[Warn] 未知指示消息 code=" << pkt.code << "\n";
                }
                print_menu_locked();
            }
        } else {
            // 未知类型
            std::lock_guard<std::mutex> iolk(g_io_mutex);
            std::cout << "\n[Warn] 未知消息类型 type=" << (int)pkt.msg_type << "\n";
            print_menu_locked();
        }
    }
    // 线程退出
    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "\n[Info] 展示线程退出.\n";
    }
}

/**
 * @brief 接收线程函数
 */
void recv_thread_func() {
    std::vector<uint8_t> buffer; // 粘包/半包处理
    Packet pkt;
    // 循环 recv_packet() 读取完整包
    while (g_running.load() && g_connected.load()) {
        int sock = g_sockfd.load();
        if (sock < 0) break;
        // 阻塞接收
        if (!recv_packet(sock, pkt, buffer)) {
            // 服务器断开
            if (!g_manual_disconnect.load() && g_running.load()) {
                std::lock_guard<std::mutex> iolk(g_io_mutex);
                std::cout << "\n[Info] 服务器连接中断.\n";
                print_menu_locked();
            }
            // 更新连接状态，关闭 socket
            g_connected.store(false);
            close_socket_if_open();
            // 防止主线程卡在 g_wait_mutex
            {
                std::lock_guard<std::mutex> lk(g_wait_mutex);
                g_waiting = false;
            }
            g_wait_cv.notify_all();
            break;
        }
        // 收到包，投递到消息队列
        {
            std::lock_guard<std::mutex> lock(g_queue_mutex);
            g_msg_queue.push(pkt);  // 把包推入 g_msg_queue，唤醒展示线程
        }
        g_queue_cv.notify_all();
    }

    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "\n[Info] 接收线程退出.\n";
    }
}

/**
 * @brief 连接服务器（功能 1）
 * 流程：
 * 1) 读取用户输入的 IP/端口
 * 2) socket() + connect()
 * 3) 设置 g_connected=true，启动接收线程
 *
 * @return true 连接成功；false 失败/已连接
 */
bool do_connect() {
    if (g_connected.load()) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Warn] 已经连接服务器.\n";
        return false;
    }
    // 若上次接收线程已退出但未 join，先回收
    if (g_recv_thread.joinable()) {
        g_recv_thread.join();
    }

    std::string ip;
    uint16_t port;
    // 读取 IP
    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "请输入服务器IP（回车默认127.0.0.1）: ";
        std::cout.flush();
    }
    std::getline(std::cin, ip);
    if (ip.empty()) ip = "127.0.0.1";
    // 读取端口
    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "请输入服务器端口（回车默认" << DEFAULT_SERVER_PORT << "）: ";
        std::cout.flush();
    }
    std::string port_str;
    std::getline(std::cin, port_str);
    if (port_str.empty()) {
        port = DEFAULT_SERVER_PORT;
    } else {
        if (!parse_u16(port_str, port)) {
            std::lock_guard<std::mutex> iolk(g_io_mutex);
            std::cout << "[Error] 端口输入非法.\n";
            return false;
        }
    }

    // 创建 socket 并 connect
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Error] 创建socket失败.\n";
        return false;
    }
    struct sockaddr_in server_addr{};
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port   = htons(port);

    // inet_pton 校验 IP 输入
    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) != 1) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Error] IP 地址格式非法.\n";
        close(sockfd);
        return false;
    }
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Error] 连接服务器失败.\n";
        close(sockfd);
        return false;
    }

    // 更新全局状态
    g_sockfd.store(sockfd);
    g_connected.store(true);
    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Info] 已连接服务器 " << ip << ":" << port << "\n";
    }

    // 启动接收线程
    g_recv_thread = std::thread(recv_thread_func);

    return true;
}

/**
 * @brief 断开连接（功能 2）
 * 流程：
 * 1) best-effort 发送 CODE_DISCONNECT
 * 2) 关闭 socket，join 接收线程
 */
void do_disconnect() {
    if (!g_connected.load()) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Warn] 当前未连接.\n";
        if (g_recv_thread.joinable()) g_recv_thread.join();
        return;
    }
    // 发一个断开请求
    uint32_t seq = 0;
    std::vector<uint8_t> empty_payload;
    send_request(CODE_DISCONNECT, empty_payload, seq);
    stop_recv_thread();
    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Info] 已断开连接.\n";
    }
}

/** @brief 获取时间（功能 3）：发送 CODE_TIME 并等待对应响应 */
void do_get_time() {
    std::vector<uint8_t> payload;
    uint32_t seq = 0;
    if (send_request(CODE_TIME, payload, seq)) {
        wait_for_response(CODE_TIME, seq);
    }
}

/** @brief 获取名字（功能 4）：发送 CODE_NAME 并等待对应响应 */
void do_get_name() {
    std::vector<uint8_t> payload;
    uint32_t seq = 0;
    if (send_request(CODE_NAME, payload, seq)) {
        wait_for_response(CODE_NAME, seq);
    }
}

/** @brief 获取客户端列表（功能 5）：发送 CODE_LIST 并等待对应响应 */
void do_get_list() {
    std::vector<uint8_t> payload;
    uint32_t seq = 0;
    if (send_request(CODE_LIST, payload, seq)) {
        wait_for_response(CODE_LIST, seq);
    }
}

/**
 * @brief 发送消息（功能 6）
 * 流程：
 * 1) 读取目标客户端编号 + 文本
 * 2) payload = target_id(4B) + text
 * 3) 发送 CODE_MESSAGE 并等待响应
 */
void do_send_message() {
    std::string id_str;
    std::string text;

    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "请输入目标客户端编号: ";
        std::cout.flush();
    }
    std::getline(std::cin, id_str);
    if (id_str.empty()) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Warn] 编号不能为空.\n";
        return;
    }

    uint32_t target_id = 0;
    if (!parse_u32(id_str, target_id)) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Warn] 编号输入非法.\n";
        return;
    }

    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "请输入要发送的内容: ";
        std::cout.flush();
    }
    std::getline(std::cin, text);
    if (text.empty()) {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Warn] 内容不能为空.\n";
        return;
    }

    // 组装 payload
    std::vector<uint8_t> payload;
    uint8_t b4[4];
    write_u32(b4, target_id);
    payload.insert(payload.end(), b4, b4 + 4);
    payload.insert(payload.end(), text.begin(), text.end());

    uint32_t seq = 0;
    if (send_request(CODE_MESSAGE, payload, seq)) {
        wait_for_response(CODE_MESSAGE, seq);
    }
}

int main() {
    // 展示线程把响应/指示打印给用户
    std::thread display_thread(display_thread_func);

    // 循环打印菜单-等待用户输入-调用对应功能的主线程
    while (g_running.load()) {
        print_menu();

        std::string choice_str;
        std::getline(std::cin, choice_str);
        if (choice_str.empty()) continue;

        int choice = 0;
        if (!parse_int(choice_str, choice)) {
            std::lock_guard<std::mutex> iolk(g_io_mutex);
            std::cout << "[Warn] 无效输入.\n";
            continue;
        }

        if (!g_connected.load()) {
            // 未连接
            if (choice == 1) {
                do_connect();
            } else if (choice == 7) {
                g_running.store(false);
                break;
            } else {
                std::lock_guard<std::mutex> iolk(g_io_mutex);
                std::cout << "[Warn] 当前未连接.\n";
            }
        } else {
            // 已连接
            switch (choice) {
                case 1: {
                    std::lock_guard<std::mutex> iolk(g_io_mutex);
                    std::cout << "[Warn] 目前已连接，可以断开再重连.\n";
                    break;
                }
                case 2:
                    do_disconnect();
                    break;
                case 3:
                    do_get_time();
                    break;
                case 4:
                    do_get_name();
                    break;
                case 5:
                    do_get_list();
                    break;
                case 6:
                    do_send_message();
                    break;
                case 7:
                    // 退出前断开连接
                    if (g_connected.load()) {
                        do_disconnect();
                    }
                    g_running.store(false);
                    break;
                default: {
                    std::lock_guard<std::mutex> iolk(g_io_mutex);
                    std::cout << "[Warn] 无效选项.\n";
                    break;
                }
            }
        }
    }

    /* 退出 */
    stop_recv_thread();
    // 防止线程线程等待卡住
    {
        std::lock_guard<std::mutex> lk(g_wait_mutex);
        g_waiting = false;
    }
    g_wait_cv.notify_all();
    // 唤醒展示线程，检查 g_running 并退出
    g_queue_cv.notify_all();
    if (display_thread.joinable()) {
        display_thread.join();
    }
    {
        std::lock_guard<std::mutex> iolk(g_io_mutex);
        std::cout << "[Info] 客户端退出.\n";
    }
    return 0;
}
