Repository for the third homework of the Communication Networks class. In this homework the students
will implement a protocol over UDP that provides reliable transport.


Etapa 1 - Protocol
Am modificat structura connection. Am adaugat:
Pentru Sender:
a) base - retine numarul de secvente din primul pachet din fereastra glisanta. Il voi folosi la libsend 
pentru a sti cand trebuie sa trimit urmatorul pachet.
b) next_to_send - contor pentru pachetul urmatorul pe care vreau sa il trimit
c) sent_packet - retine toate pachetele trimise pana acum. Cheia este numarul de secventa al pachetului 
curent, iar valoarea este pachetul curent.
Pentru receiver:
d) packet - retine toate pachetele care au venit in ordinea corecta
e) receive_packet - retine toate pachetele care au venit inainte de nr de secventa asteptat si urmeaza 
sa fie trimise. Are aceeasi structura ca sent_packet.


Etapa 2 - Conexiunea
Am implementat Three Way Handshake. Din cerinta am inteles ca sunt 3 pasi: trimiterea ACK, primirea 
SYN-ACK, trimiterea inapoi a ACK.
Implementare Sender:
Imlementarea Three-Way-Handshake este facuta in functia setup_connection. Creez pachetul initial, pun ID-ul 
corespunzator temei si tipul 2(pachetul fiind de tip SYN). Trimit pachetul pe portul 8032, oferit in cerinta 
temei. Astept raspunsul de tip SYN-ACK, verificand daca ceea ce am primit are tipul 3. Salvez ACK-ul in structura 
serverului. La sfarsit, creez un nou pachet de tip ACK, cu ack_num setat la 0, pentru ca ulterior va creste. 
Il trimit cu sendto.
Implementare Receiver:
Implementarea Three-Way-Handshake este facuta in functia wait4connect. Creez o variabila globala care va fi 
socket-ul ce va asculta si care va intercepta pachetele. Intr-o bucla, astept sa primesc pachetul de tip SYN, 
cu tipul 2. Apoi creez un nou socket pentru clientul conectat, unde setez portul la 0, pentru alegerea unui 
port liber. Trimit pachetul inapoi la client dar cu type = 3, deoarece pachetul are tipul SYN-ACK acum. 
La sfarsit, realizez ultima parte a Three Way Handshake-ului, asteptand pachetul ce contine ACK. Verific 
daca tipul este 1(adica daca este ACK). Daca nu este, atunci rulam while-ul.


Etapa 3 - Trimiterea de pachete
Cerinta sugereaza implementarea cu Selective Repeat, insa eu am facut cu Go Back n, retransmisand toate 
pachetele daca unul este pierdut. 
In libsend, iau o variabila globala care imi va contoriza nr de ferestre din fereastra glisanta. In send_data, 
verific daca trimit prea repede pachetele. De aceea pun la somn conexiunea pentru 150ms. 
Verific sa nu trec de 1000 de ferestre curente. Daca sunt peste 1000 de ferestre, blochez mutex-ul. In sender_handler
exista deja un schelet de cod care apeleaza functia recv_message_or_timeout. Aceasta functie returneaza -1 sau -14 
in caz de eroare. Daca primesc astfel de erori, blochez mutex-ul si parcurg pachetele pe care le-am adaugat in 
sent_packet pana acum. Daca primesc ACK valid, atunci sterg pachetul din istoric si mut base-ul ferestrei cu 1. 
In cazul in care pachetul cu indexul base nu are ACK, retransmit. 


Etapa 4 - API
In receiver_handler verific ce fel de pachet soseste. Daca nu are dimensiune destula, nu il prelucreaza. 
Altfel, il verifica. Daca pachetul are numarul de secventa egal cu numarul de secventa asteptat, adaug toate 
datele in packet si maresc numarul de secventa asteptat. Daca pachetul are numarul de secventa mai mare decat 
numarul de secventa asteptat, inseamna ca inaintea lui mai trebuie sa vina alte pachete. Astfel, il salvez 
in receive_packet. Functia send_data parseaza fisierul de input in pachete de dimensiune alocata si le adauga
in fereastra. Recv_data citeste datele primite. Mai intai, calculez cati bytes pot sa copiez, apoi pun datele 
in buffer-ul din argumentele functiei.. In init_receiver initializez socket-ul pe care se 
asculta si maresc buffer-ul pe care trimit. In functia init_receiver initializez socket-ul listenfd si il 
asociez cu portul dat de tema 8032. Cresc buffer-ul, chiar daca MAX_DATA_SIZE = 512. Fara aceasta marire, nu 
imi trec testele. Am facut debug si problema era ca nu impartea bine fisierul. Imi ajungea inapoi jumatate de pachet.

Cum am implementat UDP peste API sockets:
La Sender, am ales sa declar un numar de ferestre implicit. Eu am ales 600. I-am pus numarul 600. Pentru 150, 
implementarea era foarte lenta, iar pentru 1000 era prea mult, se suprapuneau pachetele si se bloca. 
Datorita usleep(150), reusesc sa gestionez corect pachetele pe care le trimit, asteptand putin timp pana se 
proceseaza(nu vreau sa risc sa mi se suprapuna pachetele si sa le pierd). 
La Receiver, am creat receive_packet in connection. Acesta retine pachetele care ajung inainte de a ajunge pachetul
cu nr de secventa asteptat. Astfel, voi astepta pana cand numarul de secventa creste si ajunge la nr de secventa din pachete.

Pentru debug, am modificat in client.cpp IP-ul inet_aton("172.16.0.100", &addr) in inet_aton("adresa_mea", &addr).
Am rulat intr-un terminal ./server si ./client checker/tests/nume_fisier. In Wireshark imi apareau pachetele care 
erau trimise si la sfarsit, dupa ce se trimitea intregul fisier, imi afisa timpul de rulare. Totusi, rezultatele sunt
inconsistente si nu inteleg de ce. 