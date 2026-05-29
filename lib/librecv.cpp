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
// file descriptor pentru socket global
static int listenfd = -1;
// dimensiunea totala a buffer-ului primit
static int buffer_size;
// vector in care retin daca s-a trimis pachetul pentru o conexiune
static int verify_connection[MAX_CONNECTIONS] = {0};

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;
    // extrag conexiunea cu ID-Ul conn_id
    struct connection *current_connection = cons[conn_id];
    pthread_mutex_lock(&cons[conn_id]->con_lock);
    // astept ca buffer-ul sa primeasca un pachet
    while (current_connection->packet.empty() && verify_connection[conn_id] == 0) {
        pthread_cond_wait(&current_connection->wait_data, &current_connection->con_lock);
    }
    // verific dimensiunea curenta de bytes neocupati din buffer
    if (!current_connection->packet.empty()) {
        size = (int) current_connection->packet.size();
        if (size > len) {
            size = len;
        }
        // copiez in buffer datele din structura
        memcpy(buffer, current_connection->packet.data(), size);
        current_connection->packet.erase(current_connection->packet.begin(), current_connection->packet.begin() + size);
    }
    /* We will write code here as to not have sync problems with recv_handler */
    // deblocam mutex-ul dupa ce am terminat de modificat conexiunea curenta
    pthread_mutex_unlock(&cons[conn_id]->con_lock);
    return size;
}

void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {
        int conn_id = -1;
        recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        // verific daca lista de conexiuni este goala
        // daca este goala, nu are rost sa ruleze degeaba si astept pana e adaugata o noua conexiune
        if (conn_id == -1)
            continue;
        if (cons.find(conn_id) == cons.end())
            continue;
        // extrag conexiunea cu ID-Ul conn_id
        struct connection *current_connection = cons[conn_id];
        pthread_mutex_lock(&current_connection->con_lock);
        // extrag header-ul din segmentul dat
        poli_tcp_data_hdr *header = (poli_tcp_data_hdr *)segment;
            // daca are tipul 4- FIN, atunci inchid conexiunea
            if (header->type == 4) {
                verify_connection[conn_id] = 1;
                // anunt pe toata lumea ca s-a terminat conexiunea
                pthread_cond_broadcast(&current_connection->wait_data);
            } else if (header->type == 0) {
                // daca pachetul e de tip 0- DATA, atunci extrag numarul de secventa si verific pachetul
                int secventa = header->seq_num;
                // daca este fix pachetul pe care il astept
                if (secventa == current_connection->expected_seq) {
                    // extrag pointer-ul si adaug pachetul in vectorul de pachete trimise cu succes
                    char *good_data = segment + sizeof(poli_tcp_data_hdr);
                    current_connection->packet.insert(current_connection->packet.end(), good_data, good_data + header->len);
                    // cresc numarul de secventa asteptat si anunt pe toata lumea ca am adaugat pachetul
                    current_connection->expected_seq++;
                    pthread_cond_broadcast(&current_connection->wait_data);
                    // extrag pachetul din conexiunea curenta
                    auto idx = current_connection->receive_packet.find(current_connection->expected_seq);
                    // daca am gasit, atunci il adaug la pachete si cresc numarul de secventa asteptat
                    while (idx != current_connection->receive_packet.end()) {
                        current_connection->packet.insert(current_connection->packet.end(), idx->second.begin(), idx->second.end());
                        current_connection->expected_seq++;
                        // elimin pachetul din lista de pachete care asteptau sa fie adaugate
                        current_connection->receive_packet.erase(idx);
                        idx = current_connection->receive_packet.find(current_connection->expected_seq);
                        pthread_cond_broadcast(&current_connection->wait_data);
                    }
                    // creez o structura pentru ACK
                    poli_tcp_ctrl_hdr header;
                    memset(&header, 0, sizeof(header));
                    header.ack_num = current_connection->expected_seq - 1;
                    header.conn_id = conn_id;
                    header.protocol_id = POLI_PROTOCOL_ID;
                    // pachetul este de tip 1 - ACK
                    header.type = 1;
                    // calculez dimensiunea ferestrei care a ramas libera in buffer-ul curent
                    header.recv_window = (buffer_size - (int) current_connection->packet.size()) / MAX_DATA_SIZE;
                    // trimit pachetul
                    sendto(current_connection->sockfd, &header, sizeof(header), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                } else if (current_connection->expected_seq < secventa) {
                    // daca nu este pachetul asteptat, adaug pachetul in vectorul care contine pachetele ce asteapta sa fie prelucrate
                    char *later_data = segment + sizeof(poli_tcp_data_hdr);
                    std::vector<char> package(later_data, later_data + header->len);
                    current_connection->receive_packet[secventa] = package;
                    // daca s-a primit un pachet in ordine
                    if (current_connection->expected_seq > 0) {
                        // creez o structura header ce trimite ACK
                        poli_tcp_ctrl_hdr header;
                        memset(&header, 0, sizeof(header));
                        header.ack_num = current_connection->expected_seq - 1;
                        header.conn_id = conn_id;
                        header.protocol_id = POLI_PROTOCOL_ID;
                        // calculez dimensiunea ferestrei
                        header.recv_window = (buffer_size - (int) current_connection->packet.size()) / MAX_DATA_SIZE;
                        // trimit pachetul
                        sendto(current_connection->sockfd, &header, sizeof(header), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                    }
                }
            }
        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    // creez o structura pe care sa primesc
    char buffer[MAX_SEGMENT_SIZE];
    // aloc structura de xonexiune
    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    new (con) struct connection();
    int conn_id = fdmax;

    // primul pas din Three Way Handshake
    // astept pachetul SYN
    // structura de client
    struct sockaddr_in client_addr;
    socklen_t clen = sizeof(client_addr);
    while (1) {
        // astept sa primesc pachet pe socket-ul declarat in init_recv
        recvfrom(listenfd, buffer, sizeof(buffer), 0, (struct sockaddr *)&client_addr, &clen);
        poli_tcp_ctrl_hdr *header = (poli_tcp_ctrl_hdr *)buffer;
        // daca pachetul este de tip SYN, atunci ies ca sa il prelucrez
        if (header->type == 2) {
            break;
        }
    }

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // se creeaza noua conexiune
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_family = AF_INET;
    // de ce 8033 + conn_id - gasesc urmatorul port disponibil
    server_addr.sin_port = htons(8033 + conn_id);
    // asociez noul socket de port
    bind(con->sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr));

    // al doilea pas din Three Way Handshake
    // trimit pachetul de tip SYN-ACK
    poli_tcp_ctrl_hdr send_syn_ack;
    memset(&send_syn_ack, 0, sizeof(send_syn_ack));
    send_syn_ack.ack_num = server_addr.sin_port;
    send_syn_ack.protocol_id = POLI_PROTOCOL_ID;
    // dimensiunea ferestrei
    send_syn_ack.recv_window = buffer_size / MAX_DATA_SIZE;
    // setez tipul pachetului la 3 - de tip SYN_ACK
    send_syn_ack.type = 3;
    // trimit pachetul
    sendto(listenfd, &send_syn_ack, sizeof(send_syn_ack), 0, (struct sockaddr *)&client_addr, clen);
    
    // al treilea pas din Three Way Handshake
    while (1) {
        struct sockaddr resp_ack;
        socklen_t clen2 = sizeof(resp_ack);
        // astept sa primesc ACK
        recvfrom(con->sockfd, buffer, sizeof(buffer), 0, (struct sockaddr *)&resp_ack, &clen2);

        poli_tcp_ctrl_hdr *resp = (poli_tcp_ctrl_hdr *)buffer;
        // daca am primit ACK, atunci ies din bucla pentru ca am primit pachetul corect
        if (resp->type == 1) {
            break;
        }
    }

    // initializez conexiunea
    con->conn_id = conn_id;
    con->expected_seq = 0;
    con->servaddr = client_addr;
    verify_connection[conn_id] = 0;

    pthread_mutex_init(&con->con_lock, NULL);
    pthread_cond_init(&con->wait_data, NULL);
    cons.insert({conn_id, con});

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


    DEBUG_PRINT("Connection established!");
    // returnez ID-ul conexiunii curente
    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */
    // pregatesc numarul de bytes disponibili
    buffer_size = recv_buffer_bytes;
    // creez socket-ul de ascultare global
    listenfd = socket(AF_INET, SOCK_DGRAM, 0);

    // creez structura serverului care va asculta pe portul 8032 si pe Ip-ul dat in tema
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(8032);
    // asociez socket-ul cu portul de ascultare
    bind(listenfd, (struct sockaddr *)&server_addr, sizeof(server_addr));

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
