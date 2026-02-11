#include "protocol.h"
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <arpa/inet.h>
#include <cerrno>

uint16_t read_u16(const uint8_t* p) {
    uint16_t v;
    std::memcpy(&v, p, sizeof(v));
    return ntohs(v);
}

uint32_t read_u32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, sizeof(v));
    return ntohl(v);
}

void write_u16(uint8_t* p, uint16_t v) {
    uint16_t net = htons(v);
    std::memcpy(p, &net, sizeof(net));
}

void write_u32(uint8_t* p, uint32_t v) {
    uint32_t net = htonl(v);
    std::memcpy(p, &net, sizeof(net));
}

/**
 * @brief 循环 send() 直到把 data 全部发完
 * @param sockfd TCP socket
 * @param data 待发送数据指针
 * @param len 待发送总长度
 * @return true 全部发送成功；false 发送失败/连接中断
 */
static bool send_all(int sockfd, const uint8_t* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sockfd, data + sent, len - sent, 0);
        if (n < 0) {
            // 被信号中断，重试
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;  // 连接异常
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool send_packet(int sockfd, const Packet& pkt) {
    // total_len = 头部 + payload
    size_t total_len = PROTO_HEADER_SIZE + pkt.payload.size();
    // length 字段限制最大包长
    if (total_len > 0xFFFF) {
        std::cerr << "[Error] Packet too large.\n";
        return false;
    }
    std::vector<uint8_t> buf(total_len);
    // 组装头部
    write_u16(&buf[0], PROTO_MAGIC);  // 0-1: magic
    buf[2] = PROTO_VERSION;  // 2: version
    buf[3] = pkt.msg_type;  // 3: msg_type
    write_u16(&buf[4], static_cast<uint16_t>(total_len));  // 4-5: total length
    write_u16(&buf[6], pkt.code);  // 6-7: code
    write_u32(&buf[8], pkt.seq);  // 8-11: seq
    // 负载 payload
    if (!pkt.payload.empty()) {
        std::memcpy(&buf[PROTO_HEADER_SIZE],
                    pkt.payload.data(),
                    pkt.payload.size());
    }
    // 发送字节流
    return send_all(sockfd, buf.data(), buf.size());
}


bool recv_packet(int sockfd, Packet& pkt, std::vector<uint8_t>& buffer) {
    uint8_t tmp[4096];
    while (true) {
        // 从 buffer 中解析
        if (buffer.size() >= PROTO_HEADER_SIZE) {
            const uint8_t* data = buffer.data();
            // magic 校验
            uint16_t magic = read_u16(data);
            if (magic != PROTO_MAGIC) {
                std::cerr << "[Error] Bad magic. Drop connection.\n";
                return false;
            }
            // version 校验
            uint8_t version = data[2];
            if (version != PROTO_VERSION) {
                std::cerr << "[Error] Bad version. Drop connection.\n";
                return false;
            }
            // length 校验
            uint16_t length = read_u16(data + 4);
            if (length < PROTO_HEADER_SIZE) {
                std::cerr << "[Error] Bad length.\n";
                return false;
            }
            // buffer 中字节数>=length可解析出一个完整包
            if (buffer.size() >= length) {
                pkt.msg_type = data[3];
                pkt.code     = read_u16(data + 6);
                pkt.seq      = read_u32(data + 8);
                size_t payload_len = length - PROTO_HEADER_SIZE;
                pkt.payload.assign(buffer.begin() + PROTO_HEADER_SIZE,
                                   buffer.begin() + PROTO_HEADER_SIZE + payload_len);
                // 移除已解析数据，保留粘包
                buffer.erase(buffer.begin(), buffer.begin() + length);
                return true;
            }
        }

        // buffer 不够一个完整包则从 socket 继续读取
        ssize_t n = recv(sockfd, tmp, sizeof(tmp), 0);
        if (n < 0) {
            // 被信号打断，重试
            if (errno == EINTR) continue;
            return false;
        }
        // 对端关闭连接
        if (n == 0) {
            return false;
        }
        // 新收到字节追加到 buffer 随后循环解析
        buffer.insert(buffer.end(), tmp, tmp + n);
    }
}
