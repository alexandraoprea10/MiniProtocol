#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <string.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int listenfd;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;
    struct connection *current_connection = cons[conn_id];
    pthread_mutex_lock(&cons[conn_id]->con_lock);
    while (current_connection->packet.empty()) {
        pthread_cond_wait(&current_connection->wait_data, &current_connection->con_lock);
    }
    /* We will write code here as to not have sync problems with recv_handler */
    int copy_buffer = len;
    if ((int)current_connection->packet.size() < len)
        copy_buffer = current_connection->packet.size();
    memcpy(buffer, current_connection->packet.data(), copy_buffer);
    current_connection->packet.erase(current_connection->packet.begin(), current_connection->packet.begin() + copy_buffer);
    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return copy_buffer;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
            if (res == -1)
                break;
            if (res <= 0)
                continue;

        } while(res == -14);
        auto idx = cons.find(conn_id);
        if (idx == cons.end()) {
            continue;
        }
        struct connection *current_connection = idx->second;
        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        if (res < (int)sizeof(struct poli_tcp_ctrl_hdr)) {
            pthread_mutex_unlock(&current_connection->con_lock);
            continue;
        }

        struct poli_tcp_data_hdr *header = (struct poli_tcp_data_hdr *)segment;
        if (header->protocol_id != POLI_PROTOCOL_ID || header->type != 0) {
            pthread_mutex_unlock(&current_connection->con_lock);
            continue;
        }

        int seq = ntohs(header->seq_num);
        int length = ntohs(header->len);
        int current_empty_space = res - sizeof(struct poli_tcp_data_hdr);
        if (length > current_empty_space) {
            length = current_empty_space;
        }
        if (length > 0) {
            char *start_buffer = segment + sizeof(struct poli_tcp_data_hdr);
            std::vector<char> buffer(start_buffer, start_buffer + length);
            if (seq == current_connection->expected_seq) {
                current_connection->packet.insert(current_connection->packet.end(), buffer.begin(), buffer.end());
                current_connection->expected_seq++;
                pthread_cond_signal(&current_connection->wait_data);
                auto idx = current_connection->receive_packet.find(current_connection->expected_seq);
                while (idx != current_connection->receive_packet.end()) {
                    current_connection->packet.insert(current_connection->packet.end(), idx->second.begin(), idx->second.end());
                    current_connection->expected_seq++;
                    current_connection->receive_packet.erase(idx);
                    idx = current_connection->receive_packet.find(current_connection->expected_seq);
                    pthread_cond_signal(&current_connection->wait_data);
                }
            } else {
                if (seq > current_connection->expected_seq || (current_connection->expected_seq > 65000 && seq < 100)) {
                    current_connection->receive_packet[seq] = buffer;
                }
            }
        }
        struct poli_tcp_ctrl_hdr resp;
        resp.ack_num = htons(current_connection->expected_seq);
        resp.protocol_id = POLI_PROTOCOL_ID;
        resp.recv_window = htons(65535);
        resp.type = 1;
        sendto(current_connection->sockfd, &resp, sizeof(resp), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
    return NULL;
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    new (con) struct connection();
    // struct connection *con = new struct connection();
    int conn_id = cons.size();

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */
    struct sockaddr_in client_addr;
    socklen_t clen = sizeof(client_addr);
    char raspuns[MAX_SEGMENT_SIZE];
    while (1) {
        int rc = recvfrom(listenfd, raspuns, sizeof(raspuns), 0, (struct sockaddr *)&client_addr, &clen);
        if (rc > 0 && ((struct poli_tcp_ctrl_hdr *)raspuns)->type == 2) {
            break;
        }
    }
    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    int buffer = 1024 * 1024;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVBUF, &buffer, sizeof(buffer));
    
    struct sockaddr_in resp;
    resp.sin_addr.s_addr = htonl(INADDR_ANY);
    resp.sin_family = AF_INET;
    resp.sin_port = 0;
    bind(con->sockfd, (struct sockaddr *)&resp, sizeof(resp));

    socklen_t clen2 = sizeof(resp);
    getsockname(con->sockfd, (struct sockaddr *)&resp, &clen2);

    struct poli_tcp_ctrl_hdr header;
    header.ack_num = resp.sin_port;
    header.protocol_id = POLI_PROTOCOL_ID;
    header.type = 3;
    sendto(listenfd, &header, sizeof(header), 0, (struct sockaddr *)&client_addr, clen);

    memcpy(&con->servaddr, &client_addr, sizeof(client_addr));
    con->expected_seq = 0;
    pthread_cond_init(&con->wait_data, NULL);
    
    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;    

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */
    listenfd = socket(AF_INET, SOCK_DGRAM, 0);

    struct sockaddr_in client_addr;
    client_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(8032);
    bind(listenfd, (struct sockaddr *)&client_addr, sizeof(client_addr));

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}