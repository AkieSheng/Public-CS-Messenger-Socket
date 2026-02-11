#ifndef LAB7_PROTOCOL_H
#define LAB7_PROTOCOL_H

#include <cstdint>
#include <cstddef>
#include <vector>

/**
 * @brief Magic Number
 */
static const uint16_t PROTO_MAGIC   = 0x5A5A;

/**
 * @brief 协议版本号
 */
static const uint8_t  PROTO_VERSION = 1;

/**
 * @brief 协议头部字段长度
 */
static const std::size_t PROTO_HEADER_SIZE = 12;

/**
 * @brief 消息类型
 * - REQUEST：客户端->服务器
 * - RESPONSE：对请求的响应
 * - INDICATION：服务器主动通知
 */
enum MsgType : uint8_t {
    MSG_TYPE_REQUEST    = 0,
    MSG_TYPE_RESPONSE   = 1,
    MSG_TYPE_INDICATION = 2
};

/**
 * @brief 功能码
 */
enum MsgCode : uint16_t {
    CODE_OK          = 0x0000, // 成功
    CODE_ERROR       = 0x0001, // 错误
    CODE_TIME        = 0x0100, // 请求/响应当前时间
    CODE_NAME        = 0x0101, // 请求/响应服务器名字
    CODE_LIST        = 0x0102, // 请求/响应客户端列表
    CODE_MESSAGE     = 0x0103, // 请求/响应发送消息
    CODE_DISCONNECT  = 0x0104, // 请求断开连接
    CODE_IND_MESSAGE = 0x0200  // 指示服务器转发的消息
};

/**
 * @brief 协议数据包
 * 字节流结构：
 * 0-1   : magic(2)
 * 2     : version(1)
 * 3     : msg_type(1)
 * 4-5   : length(2)
 * 6-7   : code(2)
 * 8-11  : seq(4)
 * 12-.. : payload(variable)
 */
struct Packet {
    /** @brief 消息类型 */
    uint8_t msg_type;
    /** @brief 功能码/业务类型 */
    uint16_t code;
    /** @brief 序列号 */
    uint32_t seq;
    /** @brief 负载数据 */
    std::vector<uint8_t> payload;
};

/**
 * @brief 从网络字节序读取 16 位无符号整数
 * @param p 指向缓冲区的指针
 * @return 主机字节序的 uint16_t
 */
uint16_t read_u16(const uint8_t* p);

/**
 * @brief 从网络字节序读取 32 位无符号整数
 * @param p 指向缓冲区的指针
 * @return 主机字节序的 uint32_t
 */
uint32_t read_u32(const uint8_t* p);

/**
 * @brief 将 16 位整数写入缓冲区
 * @param p 目标缓冲区指针
 * @param v 主机字节序数值
 */
void write_u16(uint8_t* p, uint16_t v);

/**
 * @brief 将 32 位整数写入缓冲区
 * @param p 目标缓冲区指针
 * @param v 主机字节序数值
 */
void write_u32(uint8_t* p, uint32_t v);

/**
 * @brief 序列化 Packet 并发送
 * @param sockfd TCP socket
 * @param pkt Packet 结构体（msg_type/code/seq/payload）
 * @return true 成功；false 失败
 */
bool send_packet(int sockfd, const Packet& pkt);

/**
 * @brief 从 socket + buffer 解析一个完整 Packet
 * @param sockfd TCP socket
 * @param pkt 输出：解析后的 Packet
 * @param buffer 输入输出：接收缓存，保存未解析完的字节
 * @return true 成功解析到完整包；false 连接断开/协议错误
 */
bool recv_packet(int sockfd, Packet& pkt, std::vector<uint8_t>& buffer);

#endif
