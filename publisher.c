#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdint.h>

#define CMD_PRODUCE 0x01
#define DEFAULT_PARTITION 0

int main(int argc, char *argv[]) {
    if(argc < 5) {
        printf("Uso: %s <host> <porta> <topic> <messaggio> [partizione]\n", argv[0]);
        printf("  [partizione] e' opzionale (default: 0)\n");
        return 1;
    }

    char *host = argv[1];
    char *port_str = argv[2];
    char *topic = argv[3];
    char *message = argv[4];

    uint32_t partition = (argc > 5) ? (uint32_t)atoi(argv[5]) : DEFAULT_PARTITION;

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if(getaddrinfo(host, port_str, &hints, &res) != 0){
        perror("Risoluzione host fallita");
        return 1;
    }

    int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock < 0){
        freeaddrinfo(res);
        perror("Creazione socket fallita");
        return 1;
    }

    if(connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
        close(sock);
        freeaddrinfo(res);
        perror("Connessione al broker fallita");
        return 1;
    }
    freeaddrinfo(res);
    uint8_t opcode = CMD_PRODUCE;
    write(sock, &opcode, 1);

    uint8_t topic_len = strlen(topic);
    write(sock, &topic_len, 1);
    write(sock, topic, topic_len);

    write(sock, &partition, sizeof(uint32_t));

    uint32_t payload_len = strlen(message);
    write(sock, &payload_len, sizeof(uint32_t));
    write(sock, message, payload_len);

    int64_t assigned_offset = -1;
    if (read(sock, &assigned_offset, sizeof(int64_t)) == sizeof(int64_t)){
        printf("Messaggio pubblicato! Partizione: %u, Offset assegnato: %lld\n",
               partition, (long long)assigned_offset);
    }else{
        printf("Errore nella ricezione della conferma dal broker.\n");
    }

    close(sock);
    return 0;
}
