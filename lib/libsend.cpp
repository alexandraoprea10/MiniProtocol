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
#include <string.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

// dimensiunea ferestrei
static int nr_ferestre = 150;
// vector in care retin daca s-a trimis pachetul pentru o conexiune
static int verify_connection[MAX_CONNECTIONS] = {0};

int send_data(int conn_id, char *buffer, int len)
{
    // int size = 0;
    // extrag conexiunea cu ID-Ul conn_id
    struct connection *current_connection = cons[conn_id];
    pthread_mutex_lock(&current_connection->con_lock);

    // date ramase de procesat din buffer
    char *data = buffer;
    // numarul de bytes ramasi netrimisi
    int bytes_remaining = len;

    // ca in laboratorul 7
    while (bytes_remaining > 0) {
        // lungimea datelor din pachetul curent
        int packet_len;
        // verific daca trebuie sa segmentez pachetul
        // daca nr de bytes ramasi e mai mic decat un segment, atunci ramane asa lungimea
        if (bytes_remaining < MAX_DATA_SIZE)
            packet_len = bytes_remaining;
        else 
            packet_len = MAX_DATA_SIZE;

        // creez un buffer pentru a construi header-ul
        char buffer[MAX_SEGMENT_SIZE];
        // extrag pointer-ul pentru buffer
        poli_tcp_data_hdr *header = (poli_tcp_data_hdr *)buffer;
        // setez ID-ul, lungimea header-ului, numarul de secventa
        header->conn_id = conn_id;
        header->len = packet_len;
        header->seq_num = current_connection->next_to_send;
        // tipul header-ului este de tip 0- DATA
        header->type = 0;
        memcpy(buffer + sizeof(poli_tcp_data_hdr), data, packet_len);

        // construiesc un vector de caractere cu header-ul creat
        std::vector<char> packet(buffer, buffer + sizeof(poli_tcp_data_hdr) + packet_len);
        // salvez pachetul in vectorul de pachete trimise
        current_connection->sent_packet[current_connection->next_to_send] = packet;
        // cresc numarul de secventa urmator 
        current_connection->next_to_send++;

        // merg mai departe, la urmatorul pachet
        data = data + packet_len;
        // scad numarul de bytes trimisi
        bytes_remaining = bytes_remaining - packet_len;
    }
    /* We will write code here as to not have sync problems with sender_handler */
    pthread_mutex_unlock(&current_connection->con_lock);
    // returnez lungimea nr de bytes procesati
    return len;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        // verific daca lista de conexiuni este goala
        // daca este goala, nu are rost sa ruleze degeaba si astept pana e adaugata o noua conexiune
        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
            if (res == -1) {
                // parcurg conexiunile
                for (int i = 0; i < fdmax; i++) {
                    // daca nu exista conexiune nu intru in urmatorul for
                    if (cons.find(i) == cons.end())
                        continue;
                    // extrag conexiune curenta
                    struct connection *current_connection = cons[i];
                    pthread_mutex_lock(&current_connection->con_lock);
                    // parcurg pachetele din conexiunea curenta
                    // am ales sa fac Go Back n
                    for (int j = current_connection->base; j < current_connection->next_to_send; j++) {
                        // daca gasesc pachetul cu indexul numarului de secventa din for, atunci il trimit
                        if (current_connection->sent_packet.find(j) != current_connection->sent_packet.end()) {
                            sendto(current_connection->sockfd, current_connection->sent_packet[j].data(), current_connection->sent_packet[j].size(), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                        }
                    }
                    pthread_mutex_unlock(&current_connection->con_lock);
                }
            }
        } while(res == -14);

        if (conn_id == -1)
            continue;
        if (cons.find(conn_id) == cons.end())
            continue;

        // extrag conexiunea curenta cu ID-ul conn_id
        struct connection *current_connection = cons[conn_id];
        // blocare mutex ca sa nu modific starea
        pthread_mutex_lock(&current_connection->con_lock);
        // extrag pachetul din buffer
        if (res >= (int) sizeof(struct poli_tcp_ctrl_hdr)) {
            // verific daca ppachetul are ID-ul corespunzator si daca tipul pachetului este 1
            poli_tcp_ctrl_hdr *header = (poli_tcp_ctrl_hdr *)buf;
            if (header->protocol_id == POLI_PROTOCOL_ID && header->type == 1) {
                // extrag ACK
                int value_ack = header->ack_num;
                // parcurg pachetele conformate si le sterg din map-ul pentru sent_packet
                for (int k = current_connection->base; k <= value_ack; k++) {
                    current_connection->sent_packet.erase(k);
                }
                // daca trec de baza cu ack, atunci actualizez baza ferestrei
                if (value_ack + 1 > current_connection->base) {
                    current_connection->base = value_ack + 1;
                }
                // verific daca pachetul ACK contine o dimensiune corecta pentru primire
                if (header->recv_window > 0)
                current_connection->max_window_seq = header->recv_window;
                // parcurg pachetele
                for (int k = current_connection->base; k < current_connection->base + current_connection->max_window_seq; k++) {
                    // daca pachetul are numarul de secventa in vector, atunci inseamna ca nu l am sters i trebuie trimis
                    if (current_connection->sent_packet.find(k) != current_connection->sent_packet.end()) {
                        sendto(current_connection->sockfd, current_connection->sent_packet[k].data(), current_connection->sent_packet[k].size(), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                    }
                }
                // verific daca am trimis toate datele
                if (verify_connection[conn_id] == 0 && current_connection->sent_packet.empty() && current_connection->base > 0 && current_connection->base == current_connection->next_to_send) {
                    // daca da, atunci trimit sfarsitul conexiunii
                    poli_tcp_data_hdr header;
                    memset(&header, 0, sizeof(header));
                    header.conn_id = conn_id;
                    header.protocol_id = POLI_PROTOCOL_ID;
                    // setam tipul la 4 - FIN
                    header.type = 4;
                    // trimit pachetul
                    sendto(current_connection->sockfd, &header, sizeof(header), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                    // s-a trimis pachetul, inchidem
                    verify_connection[conn_id] = 1;
                }
            }
        }
        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */
        // deblochez mutex-ul dupa modificari
        pthread_mutex_unlock(&current_connection->con_lock);
    }
    return NULL;
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */
    // creez o noua structura (daca lasam doar alocarea, nu treceau testele)
    // am cautat documentatia din C++ si trebuie apelat si un constructie
    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    new (con) struct connection();
    // struct connection *con = new struct connection();
    int conn_id = 0;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    // initializez campurile structurii create mai sus
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_port = port;

    // primul pas pentru Three Way Handshake(trimit SYN pe portul 8032)
    // creez un alt pachet pentru conexiune
    poli_tcp_ctrl_hdr send_syn;
    memset(&send_syn, 0, sizeof(send_syn));
    send_syn.conn_id = 0;
    send_syn.protocol_id = POLI_PROTOCOL_ID;
    // setez tipul pachetului la 2(de tip SYN)
    send_syn.type = 2;
    // trimit pachetul
    sendto(con->sockfd, &send_syn, sizeof(send_syn), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

    // al doilea pas pentru Three Way Handshake(primesc SYN-ACK)
    // aloc o memorie pentur un nou buffer pentru pachetul pe care il voi primi
    char buff[MAX_SEGMENT_SIZE];
    while (1) {
        struct sockaddr_in resp;
        socklen_t clen = sizeof(resp);
        // primesc pachetul
        recvfrom(con->sockfd, buff, sizeof(buff), 0, (struct sockaddr *)&resp, &clen);
        poli_tcp_ctrl_hdr *header = (poli_tcp_ctrl_hdr *)buff;
        // verific daca tipul pachetului este SYN-ACK
        if (header->type == 3) {
            // actualizez portul serverului du noul port dat
            if (header->ack_num != 0) {
                con->servaddr.sin_port = header->ack_num;
            }
            // setez fereastra maxima
            if (header->recv_window > 0) {
                con->max_window_seq = header->recv_window;
            } else {
                con->max_window_seq = nr_ferestre;
            }
            break;
        }
    }
    
    // setez ID-ul conexiunii curente cu fdmax- conexiune globala
    conn_id = fdmax;

    // al treilea pas din Three Way Handshake
    // trimit ultimul ACK
    // creez o structura noua
    poli_tcp_ctrl_hdr send_last_ack;
    memset(&send_last_ack, 0, sizeof(send_last_ack));
    send_last_ack.conn_id = conn_id;
    send_last_ack.protocol_id = POLI_PROTOCOL_ID;
    // initializez tipul, de tip ACK
    send_last_ack.type = 1;
    // trimit pachetul
    sendto(con->sockfd, &send_last_ack, sizeof(send_last_ack), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
    
    // initializez baza, nr de secventa si ID-ul conexiunii
    con->base = 0;
    con->next_to_send = 0;
    con->conn_id = conn_id;
    verify_connection[conn_id] = 0;
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
    // am modificat timpul, pentru ca trimit destul de multe pachete si nu vreau sa se blocheze
    spec.it_value.tv_nsec = 40000000;    
    spec.it_interval.tv_sec = 0;    
    spec.it_interval.tv_nsec = 40000000;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    // initializez variabila de conditie si mutex-ul de initializare(deja facut)
    pthread_mutex_init(&con->con_lock, NULL);
    pthread_cond_init(&con->wait_data, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
