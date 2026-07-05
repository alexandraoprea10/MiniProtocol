Repository for the third homework of the Communication Networks class. In this homework the students
will implement a protocol over UDP that provides reliable transport.


Etapa 1 - Protocol

Cum am codificat tipul pachetelor:
DATA = 0
ACK = 1
SYN = 2
SYN_ACK = 3
FIN = 4

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
f) wait_data - o variabila ce respecta o conditie(am folosit-o la SO de multe ori). Daca nu primim niciun 
pachet, atunci o blochez cu pthread_cond_wait. In receiver_handler o mai apelez odata, ca in cazul in care 
recv_data "doarme", sa o "trezeasca" pentru a pune datele.


Etapa 2 - Conexiunea


Am implementat Three Way Handshake. Din cerinta am inteles ca sunt 3 pasi: trimiterea SYN, primirea 
SYN-ACK, trimiterea inapoi a ACK.
Implementare Sender:
Imlementarea Three-Way-Handshake este facuta in functia setup_connection. Creez pachetul initial, pun ID-ul 
corespunzator temei si tipul 2(pachetul fiind de tip SYN). Trimit pachetul pe portul 8032, oferit in cerinta 
temei. Astept raspunsul de tip SYN-ACK, verificand daca ceea ce am primit are tipul 3. Salvez ACK-ul in structura 
serverului. La sfarsit, creez un nou pachet de tip ACK, cu ack_num setat la 0.
Il trimit cu sendto.
Implementare Receiver:
Implementarea Three-Way-Handshake este facuta in functia wait4connect. Creez o variabila globala care va fi 
socket-ul ce va asculta si care va intercepta pachetele. Intr-o bucla, astept sa primesc pachetul de tip SYN, 
cu tipul 2. Apoi creez un nou socket pentru clientul conectat, unde setez ca fiind primul port disponibil dupa 8032,
pentru alegerea unui port liber. Trimit pachetul inapoi la client dar cu type = 3, deoarece pachetul are tipul SYN-ACK acum. 
La sfarsit, realizez ultima parte a Three Way Handshake-ului, asteptand pachetul ce contine ACK. Verific 
daca tipul este 1(adica daca este ACK). Daca nu este, atunci rulam while-ul.


Etapa 3 - Trimiterea de pachete


Cerinta sugereaza implementarea cu Selective Repeat, insa eu am facut cu Go Back n, retransmisand toate 
pachetele daca unul este pierdut. 
In libsend, iau o variabila globala care imi va contoriza nr de ferestre din fereastra glisanta. Pe cazul 
de timeout, ma intorc la secventa base si tetransmit toate pachetele de la base la next_to_send. Caut 
pachetul cu find. Daca primesc ACK, atunci sterg pachetele confirmate. 
Verific sa nu trec de next_to_send ca sa nu creez pachete fara nimic in ele. Pachetul creat are valoarea 0 - 
este de tip DATA.
In librecv, am o implementare mai ciudata. Salvez pachetele doar cand nu sunt cele pe care le caut. Daca numarul 
de secventa este mai mare decat numarul de secventa asteptat, atunci le adaug intr-un vector de pachete care urmeaza 
a fi interceptate. Astept pana vine pachetul cu numarul de secventa pe care mi-l doresc eu.
Am folosit si un vector global verify_connection, pe care il setez la 0 la inceputul oricarei conexiuni. Cand toate 
pachetele din sent_packet au fost trimise, trimit un pachet de tipul 4 - FIN, care anunta ca am trimis tot si modific 
valoarea verify_connection[conn_id] = 1, adica opresc conexiunea.


Etapa 4 - API


In receiver_handler verific ce fel de pachet soseste. Daca nu are dimensiune destula, nu il prelucreaza. 
Altfel, il verifica. Daca pachetul are numarul de secventa egal cu numarul de secventa asteptat, adaug toate 
datele in packet si maresc numarul de secventa asteptat. Daca pachetul are numarul de secventa mai mare decat 
numarul de secventa asteptat, inseamna ca inaintea lui mai trebuie sa vina alte pachete. Astfel, il salvez 
in receive_packet. Functia send_data parseaza fisierul de input in pachete de dimensiune alocata si le adauga
in fereastra. Recv_data citeste datele primite. Mai intai, calculez cati bytes pot sa copiez, apoi pun datele 
in buffer-ul din argumentele functiei.Cand primesc un pachet de tip FIN, inchid conexiunea la conexiunea cu ID-ul 
conn_id si anunt pe toata lumea ca am facut aceasta modificare. Astfel, daca recv_data astepta, el iese din bucla s
returneaza 0(pt ca s-a inchis conexiunea). In init_receiver initializez socket-ul pe care se 
asculta si maresc buffer-ul pe care trimit. In functia init_receiver initializez socket-ul listenfd si il 
asociez cu portul dat de tema 8032. 

Cum am implementat UDP peste API sockets


La Sender, am ales sa declar un numar de ferestre implicit. Eu am ales 150. Pentru 150, pachetele se trimit destul de bine. 
Am pus un timer de 40ms intre ferestre, pentru retransmisie, ca sa nu se suprapuna si sa nu se piarda. 
Inainte pusesem 600 de pachete, trecea doar local, pe masina crapa, probabil facea overflow.
La Receiver, am creat receive_packet in connection. Acesta retine pachetele care ajung inainte de a ajunge pachetul
cu nr de secventa asteptat. Astfel, voi astepta pana cand numarul de secventa creste si ajunge la nr de secventa din pachete.



Pentru debug, am modificat in client.cpp IP-ul inet_aton("172.16.0.100", &addr) in inet_aton("adresa_mea", &addr).
Am rulat intr-un terminal ./server si ./client checker/tests/nume_fisier. In Wireshark imi apareau pachetele care 
erau trimise si la sfarsit, dupa ce se trimitea intregul fisier, imi afisa timpul de rulare. Totusi, rezultatele sunt
inconsistente si nu inteleg de ce. 
