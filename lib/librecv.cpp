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
// creez un socket prin care receiver-ul asteapta pachete
int listenfd;

int recv_data(int conn_id, char *buffer, int len)
{
    // int size = 0;
    // extragem conexiunea cu ID-ul conn_id
    struct connection *current_connection = cons[conn_id];
    // blochez mutex-ul pentru ulterioarele modificari ce vin asupra pachetului
    pthread_mutex_lock(&cons[conn_id]->con_lock);
    // astept ca buffer-ul sa primeasca un pachet
    while (current_connection->packet.empty()) {
        pthread_cond_wait(&current_connection->wait_data, &current_connection->con_lock);
    }
    /* We will write code here as to not have sync problems with recv_handler */
    // initializam nr de bytes cu lungimea initiala oferita ca argument
    int copy_buffer = len;
    // daca lungimea este mai mica, o actualizam
    if ((int)current_connection->packet.size() < len)
        copy_buffer = current_connection->packet.size();
    // copiez in buffer data-ul din pachet(pachetul contine header + data)
    memcpy(buffer, current_connection->packet.data(), copy_buffer);
    // stergem datele pe care tocmai le-am copiat
    current_connection->packet.erase(current_connection->packet.begin(), current_connection->packet.begin() + copy_buffer);
    // deblocam mutex-ul dupa ce am terminat de modificat conexiunea curenta
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
            // daca gasim vreo eroare, nu facem nimic
            // daca res == -1, atunci am primit o eroare si iesim din bucla
            if (res == -1)
                break;
            // daca res <= 0, atunci am primit un pachet gol si trebuie sa rulam pana primim ceva
            if (res <= 0)
                continue;

        } while(res == -14);
        // caut pachetul in ceea ce am primit deja. daca apartine, atunci trecem mai departe
        auto idx = cons.find(conn_id);
        if (idx == cons.end()) {
            continue;
        }
        // extragem structura din pachetul gasit
        struct connection *current_connection = idx->second;
        // blocam mutex-ul pentru ulterioarele modificari ale conexiunii curente
        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        // daca n-am primit pachet complet, nu il modific si trec mai departe
        if (res < (int)sizeof(struct poli_tcp_ctrl_hdr)) {
            pthread_mutex_unlock(&current_connection->con_lock);
            continue;
        }
        
        // daca am primit pachet complet, atunci il extrag din buffer-ul curent
        struct poli_tcp_data_hdr *header = (struct poli_tcp_data_hdr *)segment;
        // daca pachetul nu respecta ID-ul corespunzator sau nu are tipul 0, nu il modific si trec mai departe
        if (header->protocol_id != POLI_PROTOCOL_ID || header->type != 0) {
            pthread_mutex_unlock(&current_connection->con_lock);
            continue;
        }

        // extrag numarul de secventa si lungimea datelor pusa in header
        int seq = ntohs(header->seq_num);
        int length = ntohs(header->len);
        // veridic numarul de bytes din pachet
        int current_empty_space = res - sizeof(struct poli_tcp_data_hdr);
        // daca am ceva mai mare decat cat avm liber, trunchiez la ce putem primi
        if (length > current_empty_space) {
            length = current_empty_space;
        }
        if (length > 0) {
            // calculez de unde incep datele din pachet
            char *start_buffer = segment + sizeof(struct poli_tcp_data_hdr);
            // creez un vector in care pun doar datele care mi trebuie
            std::vector<char> buffer(start_buffer, start_buffer + length);
            // daca pachetul are numarul de secventa egal cu ceea ce astept 
            if (seq == current_connection->expected_seq) {
                // adaug noul pachet si incrementez numarul de secventa asteptat
                current_connection->packet.insert(current_connection->packet.end(), buffer.begin(), buffer.end());
                current_connection->expected_seq++;
                pthread_cond_signal(&current_connection->wait_data);
                // caut daca exista deja pachetul, pentru a nu-l pune de doua ori
                auto idx = current_connection->receive_packet.find(current_connection->expected_seq);
                while (idx != current_connection->receive_packet.end()) {
                    // adaug pachetul la sfarsitul vectorului 
                    current_connection->packet.insert(current_connection->packet.end(), idx->second.begin(), idx->second.end());
                    current_connection->expected_seq++;
                    // sterg pachetul mutat mai sus pentru a fi codul mai rapid
                    current_connection->receive_packet.erase(idx);
                    idx = current_connection->receive_packet.find(current_connection->expected_seq);
                    pthread_cond_signal(&current_connection->wait_data);
                }
            } else {
                // daca pachetul are nr de secventa mai mare decat ceea ce astept, atunci il adaug in lista de asteptare pana primesc ceea ce lipseste deja
                if (seq > current_connection->expected_seq || (current_connection->expected_seq > 65000 && seq < 100)) {
                    current_connection->receive_packet[seq] = buffer;
                }
            }
        }
        // creez pachetul de raspuns catre sender
        struct poli_tcp_ctrl_hdr resp;
        resp.ack_num = htons((uint16_t) seq);
        resp.protocol_id = POLI_PROTOCOL_ID;
        resp.recv_window = htons(65535);
        // pun tipul pachetului(de tip ACK)
        resp.type = 1;
        // trimit pachetul
        sendto(current_connection->sockfd, &resp, sizeof(resp), 0, (struct sockaddr *)&current_connection->servaddr, sizeof(current_connection->servaddr));
        // deblochez mutex-ul dupa modificari
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
    return NULL;
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    // creez o noua structura (daca lasam doar alocarea, nu treceau testele)
    // am cautat documentatia din C++ si trebuie apelat si un constructie
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

    // creez o structura a clientului si astept raspunsul de pe socket
    struct sockaddr_in client_addr;
    socklen_t clen = sizeof(client_addr);
    char raspuns[MAX_SEGMENT_SIZE];
    // primul pas din Three Way Handshake
    // Primesc pachetul SYN
    while (1) {
        int rc = recvfrom(listenfd, raspuns, sizeof(raspuns), 0, (struct sockaddr *)&client_addr, &clen);
        // verific daca pachetul este de tip SYN
        if (rc > 0 && ((struct poli_tcp_ctrl_hdr *)raspuns)->type == 2) {
            break;
        }
    }
    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    // am modificat buffer-ul si l-am setat la 1024, chiar daca MAX_SEGMENT_SIZE este 512
    // l-am marit ca sa ma asigur ca este destul spatiu si impart corect pachetele
    int buffer = 1024 * 1024;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVBUF, &buffer, sizeof(buffer));
    
    // creez o structura de raspuns
    struct sockaddr_in resp;
    // initializez campurile ca sa avem orice IP si port.
    resp.sin_addr.s_addr = htonl(INADDR_ANY);
    resp.sin_family = AF_INET;
    resp.sin_port = 0;
    // caut apchetul 
    bind(con->sockfd, (struct sockaddr *)&resp, sizeof(resp));

    // verific ce port a fost ales de socket
    socklen_t clen2 = sizeof(resp);
    getsockname(con->sockfd, (struct sockaddr *)&resp, &clen2);

    // al doilea pas pentru Three Way Handshake este trimiterea pachetului de tipul SYN_ACK
    // creez o structura pentru raspuns
    struct poli_tcp_ctrl_hdr header;
    header.ack_num = resp.sin_port;
    header.protocol_id = POLI_PROTOCOL_ID;
    // type = 3 deoarece pachetul este de tip SYN_ACK
    header.type = 3;
    // trimit pachetul inapoi la client
    sendto(listenfd, &header, sizeof(header), 0, (struct sockaddr *)&client_addr, clen);

    // al treilea pas pentru Three Way Handshake este primirea ACK-ului final
    while (1) {
        // creez o structura pe care sa primesc si aloc un buffer pentru raspuns
        char resp[MAX_SEGMENT_SIZE];
        struct sockaddr_in server_addr;
        socklen_t clen3 = sizeof(server_addr);
        int rc = recvfrom(con->sockfd, resp, sizeof(resp), 0, (struct sockaddr *)&server_addr, &clen);
        if (rc >= (int)sizeof(struct poli_tcp_ctrl_hdr)) {
            struct poli_tcp_ctrl_hdr *header_final = (struct poli_tcp_ctrl_hdr *)resp;
            // verific daca tipul este de tip ACK si daca respecta protocolul
            if (header_final->type == 1 && header_final->protocol_id == POLI_PROTOCOL_ID) {
                break;
            }
        }
    }
    // salvez clientul ca sa stiu cui trimit
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
    // nu am modificat timpul aici, pentru ca nu imi mai treceau testele     
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
    // creez conexiunea
    listenfd = socket(AF_INET, SOCK_DGRAM, 0);

    // am modificat buffer-ul si l-am setat la 1024, chiar daca MAX_SEGMENT_SIZE este 512
    // l-am marit ca sa ma asigur ca este destul spatiu si impart corect pachetele
    int buffer = 1024 * 1024;
    setsockopt(listenfd, SOL_SOCKET, SO_RCVBUF, &buffer, sizeof(buffer));

    // creez o structura a server-ului
    struct sockaddr_in server_addr;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(8032);
    // caut pachetele de pe portul 8032, cu pachete de orice tip si orice IP
    bind(listenfd, (struct sockaddr *)&server_addr, sizeof(server_addr));

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}