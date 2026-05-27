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

// dimensiunea ferestrei
int nr_ferestre = 600;

int send_data(int conn_id, char *buffer, int len)
{
    // extragem conexiunea cu ID-ul conn_id
    struct connection *current_connection = cons[conn_id];
    int ferestre_curente = 0;
    while (1) {
        pthread_mutex_lock(&current_connection->con_lock);
        // daca nu s-a umplut fereastra, ma opresc si ies
        if (current_connection->next_to_send - current_connection->base < nr_ferestre) {
            break;
        }
        // deblochez mutex-ul pentru a primi ACK
        pthread_mutex_unlock(&current_connection->con_lock);
        // cresc nr de ferestre
        ferestre_curente++;
        // daca primesc prea multe ferestre, blochez mutex-ul si ies
        if (ferestre_curente > 1000) {
            pthread_mutex_lock(&current_connection->con_lock);
            break;
        }
        // astept 150ms pentru a verifica din nou(trimit multe pachete deodata si sa nu se suprapuna)
        usleep(150);
    }

    /* We will write code here as to not have sync problems with sender_handler */
    // creez pachetul pe care vreau sa il trimit
    struct poli_tcp_data_hdr header;
    header.conn_id = current_connection->conn_id;
    header.len = htons(len);
    header.protocol_id = POLI_PROTOCOL_ID;
    header.seq_num = htons(current_connection->next_to_send);
    header.type = 0;

    // creez un vector care sa contine header-ul si datele
    std::vector<char> send_packet(sizeof(header) + len);
    // inital, copiez header-ul
    memcpy(send_packet.data(), &header, sizeof(header));
    // apoi, copiez datele dupa header
    memcpy(send_packet.data() + sizeof(header), buffer, len);

    // salvez pachetul creat, in cazul in care se primeste un ACK gresit si este nevoie sa l retransmit
    current_connection->sent_packet[current_connection->next_to_send] = send_packet;
    current_connection->next_to_send++;

    // trimit pachetul(ca in laboratorul 6/7)
    int rc = sendto(current_connection->sockfd, send_packet.data(), send_packet.size(), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
    // deblochez mutex-ul dupa ce am terminat de modificat conexiunea curenta
    pthread_mutex_unlock(&current_connection->con_lock);
    // returnez lungimea datelor pe care am trimis-o
    return len;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        // verific daca lista de conexiuni este goala
        // daca este goala, nu are rost sa las sa ruleze degeaba si astept pana este adaugata o conexiune
        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
            if (res == -1 || res == -14) {
                // caut conexiunea cu ID-ul 0
                auto again = cons.find(0);
                if (again != cons.end()) {
                    struct connection *current_connection = again->second;
                    pthread_mutex_lock(&current_connection->con_lock);
                    // verific daca exista pachete care nu sunt gata de trimis
                    if (!current_connection->sent_packet.empty()) {
                        auto idx2 = current_connection->sent_packet.begin();
                        // am implementat Go Back n - stiu ca era recomandat Selective Repeat, dar nu am reusit sa il fac sa ruleze corect
                        // parcurg toate pachetele pentru care nu am primit ACK
                        while (idx2 != current_connection->sent_packet.end()) {
                            // extragem pachetul curent
                            const std::vector <char>& packet = idx2->second;
                            // trimit pachetul din nou
                            sendto(current_connection->sockfd, packet.data(), packet.size(), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                            idx2++;
                        }
                    }
                    pthread_mutex_unlock(&current_connection->con_lock);
                }
            }
        } while(res == -14);

        auto idx = cons.find(conn_id);
        if (idx == cons.end()) {
            continue;
        }
        // extrag conexiunea valida
        struct connection *current_connection = idx->second;
        // blocare mutex ca sa nu modific starea
        pthread_mutex_lock(&current_connection->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */
        if (res >= (int)sizeof(struct poli_tcp_ctrl_hdr)) {
            // extrag pachetul din buffer
            struct poli_tcp_ctrl_hdr *header = (struct poli_tcp_ctrl_hdr*) buf;
            // verific daca ppachetul are ID-ul corespunzator si daca tipul pachetului este 1
            if (header->protocol_id == POLI_PROTOCOL_ID && header->type == 1) {
                // extrag ACK
                int ack = ntohs(header->ack_num);
                // sterg pachetul cu ACK-ul respectiv din vectorul de pachete
                current_connection->sent_packet.erase(ack);
                int old = current_connection->base;
                // merg mai departe cu urmatorul pachet- glisare
                while (current_connection->base < current_connection->next_to_send) {
                    int idx2 = current_connection->base;
                    // daca pachetul inca nu a primit ACK, iesim din bucla, trebuie retransmis
                    if (current_connection->sent_packet.count(idx2) > 0) {
                        break;
                    }
                    current_connection->base++;
                }
                // verific daca s-a mutat fereastra
                if (current_connection->base == old && ack > current_connection->base) {
                    const std::vector<char>& packet = current_connection->sent_packet[current_connection->base];
                    // daca da, atunci pachetul nu a primit ACK corect si trebuie retransmis
                    sendto(current_connection->sockfd, packet.data(), packet.size(), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
                }
            }
        }
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

    // am modificat buffer-ele si le-am setat la 1024, chiar daca MAX_SEGMENT_SIZE este 512
    // le-am marit ca sa ma asigur ca este destul spatiu si impart corect pachetele
    int buffer = 1024 * 1024;
    setsockopt(con->sockfd, SOL_SOCKET, SO_SNDBUF, &buffer, sizeof(buffer));
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVBUF, &buffer, sizeof(buffer));

    // initializez campurile structurii create mai sus
    con->base = 0;
    con->next_to_send = 0;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_port = port;

    // primul pas pentru Three Way Handshake(trimit SYN pe portul 8083)
    // creez un alt pachet pentru conexiune
    struct poli_tcp_ctrl_hdr header;
    header.protocol_id = POLI_PROTOCOL_ID;
    // setez tipul pachetului la 1(de tip SYN)
    header.type = 2;
    // trimit pachetul
    sendto(con->sockfd, &header, sizeof(header), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

    // al doilea pas pentru Three Way Handshake(primesc SYN-ACK)
    // aloc o memorie pentur un nou buffer pentru pachetul pe care il voi primi
    char buff[MAX_SEGMENT_SIZE];
    struct sockaddr_in client_addr;
    socklen_t clen = sizeof(client_addr);
    int rc = recvfrom(con->sockfd, buff, sizeof(buff), 0, (struct sockaddr *)&client_addr, &clen);

    if (rc >= (int)sizeof(struct poli_tcp_ctrl_hdr)) {
        struct poli_tcp_ctrl_hdr *resp = (struct poli_tcp_ctrl_hdr *)buff;
        // salvam ce am primit, ca sa stim unde trimitem restul pachetelor
        con->servaddr.sin_port = resp->ack_num;
        // al treilea pas pentru Three Way Handshake este trimiterea ACK-ului
        // protocolul foloseste pachete de tip header, asa ca voi trimite ACK printr-un astfel de pachet
        struct poli_tcp_ctrl_hdr header_final;
        // pornesc cu ACK de la 0
        header_final.ack_num = htons(0);
        // pun protocolul specificat in tema
        header_final.protocol_id = POLI_PROTOCOL_ID;
        // pun tipul pachetului la 1(de tip ACK)
        header_final.type = 1;
        // trimit pachetul
        sendto(con->sockfd, &header_final, sizeof(header_final), 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
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
    // am modificat timpul la 20000000 pentru ca sunt multe pachete si sa nu se suprapuna
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
