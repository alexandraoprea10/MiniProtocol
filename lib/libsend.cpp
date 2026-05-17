#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <vector>
#include <string.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int nr_ferestre = 600;

int send_data(int conn_id, char *buffer, int len)
{
    struct connection *current_connection = cons[conn_id];
    int ferestre_curente = 0;
    while (1) {
        pthread_mutex_lock(&current_connection->con_lock);
        if (current_connection->next_to_send - current_connection->base < nr_ferestre) {
            break;
        }
        pthread_mutex_unlock(&current_connection->con_lock);
        ferestre_curente++;
        if (ferestre_curente > 1000) {
            pthread_mutex_lock(&current_connection->con_lock);
            break;
        }
        usleep(100);
    }

    /* We will write code here as to not have sync problems with sender_handler */
    struct poli_tcp_data_hdr header;
    header.conn_id = current_connection->conn_id;
    header.len = htons(len);
    header.protocol_id = POLI_PROTOCOL_ID;
    header.seq_num = htons(current_connection->next_to_send % 65536);
    header.type = 0;

    std::vector<char> send_packet(sizeof(header) + len);
    memcpy(send_packet.data(), &header, sizeof(header));
    memcpy(send_packet.data() + sizeof(header), buffer, len);

    current_connection->sent_packet[current_connection->next_to_send] = send_packet;
    current_connection->next_to_send++;

    int rc = sendto(current_connection->sockfd, send_packet.data(), send_packet.size(), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
    pthread_mutex_unlock(&current_connection->con_lock);
    if (rc < 0)
        return -1;

    return len;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
            if (res == -1 || res == -14) {
                auto again = cons.find(0);
                if (again != cons.end()) {
                    struct connection *current_connection = again->second;
                    pthread_mutex_lock(&current_connection->con_lock);

                    if (!current_connection->sent_packet.empty()) {
                        auto idx2 = current_connection->sent_packet.begin();
                        while (idx2 != current_connection->sent_packet.end()) {
                            const std::vector <char>& packet = idx2->second;
                            sendto(current_connection->sockfd, packet.data(), packet.size(), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                            idx2++;
                        }
                    }
                    pthread_mutex_unlock(&current_connection->con_lock);
                }
            }
        } while(res == -14);

        if (res <= 0 || conn_id == -1) {
            continue;
        }
        auto idx = cons.find(conn_id);
        if (idx == cons.end()) {
            continue;
        }
        struct connection *current_connection = idx->second;
        pthread_mutex_lock(&current_connection->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */
        if (res >= (int)sizeof(struct poli_tcp_ctrl_hdr)) {
            struct poli_tcp_ctrl_hdr *header = (struct poli_tcp_ctrl_hdr*) buf;
            if (header->protocol_id == POLI_PROTOCOL_ID && header->type == 1) {
                int ack = ntohs(header->ack_num);
                current_connection->sent_packet.erase(ack);
                int old = current_connection->base;
                while (current_connection->base < current_connection->next_to_send) {
                    int idx2 = current_connection->base;
                    if (current_connection->sent_packet.count(idx2) > 0) {
                        break;
                    }
                    current_connection->base++;
                }
                if (current_connection->base == old && ack > current_connection->base) {
                    const std::vector<char>& packet = current_connection->sent_packet[current_connection->base];
                    sendto(current_connection->sockfd, packet.data(), packet.size(), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                }
            }
        }
        pthread_mutex_unlock(&current_connection->con_lock);
    }
    return NULL;
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    new (con) struct connection();
    // struct connection *con = new struct connection();
    int conn_id = 0;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    int buffer = 1024 * 1024;
    setsockopt(con->sockfd, SOL_SOCKET, SO_SNDBUF, &buffer, sizeof(buffer));
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVBUF, &buffer, sizeof(buffer));

    con->base = 0;
    con->next_to_send = 0;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_port = port;

    struct poli_tcp_ctrl_hdr header;
    header.protocol_id = POLI_PROTOCOL_ID;
    header.type = 2;
    sendto(con->sockfd, &header, sizeof(header), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

    char buff[MAX_SEGMENT_SIZE];
    struct sockaddr_in client_addr;
    socklen_t clen = sizeof(client_addr);
    int rc = recvfrom(con->sockfd, buff, sizeof(buff), 0, (struct sockaddr *)&client_addr, &clen);

    if (rc >= (int)sizeof(struct poli_tcp_ctrl_hdr)) {
        struct poli_tcp_ctrl_hdr *resp = (struct poli_tcp_ctrl_hdr *)buff;
        con->servaddr.sin_port = resp->ack_num;
    }
    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 20000000;    
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 20000000;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;


    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return 0;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;
    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
