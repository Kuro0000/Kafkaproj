#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdint.h>

#define CMD_FETCH 0x02
#define CMD_COMMIT 0x03

typedef struct {
    uint64_t offset;
    uint32_t length;
} MessageHeader;

int main(int argc, char *argv[]) {
    if (argc < 6) {
        printf("Uso: %s <host> <porta> <topic> <partizione> <offset_iniziale>\n", argv[0]);
        return 1;
    }
    char *host=argv[1];
    char *port_str=argv[2];
    char *topic =argv[3];
    uint32_t partition = (uint32_t)strtoul(argv[4], NULL, 10);
    uint64_t target_offset = strtoull(argv[5], NULL, 10);

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port_str, &hints, &res) != 0) {
        perror("Risoluzione host fallita");
        return 1;
    }
    int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock < 0) {
        freeaddrinfo(res);
        perror("Creazione socket fallita");
        return 1;
    }

    if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
        close(sock);
        freeaddrinfo(res);
        perror("Connessione al broker fallita");
        return 1;
    }
    freeaddrinfo(res);

    uint8_t opcode =CMD_FETCH;
    write(sock, &opcode, 1);
    uint8_t topic_len = strlen(topic);
    write(sock, &topic_len, 1);
    write(sock, topic, topic_len);

    write(sock, &partition, sizeof(uint32_t));
    write(sock, &target_offset, sizeof(uint64_t));

    printf("In ascolto sul topic '%s' partizione %u, a partire dall'offset %lu...\n",
           topic, partition, target_offset);

    MessageHeader header;
    while(read(sock, &header, sizeof(MessageHeader)) == sizeof(MessageHeader)) {
        char *payload = malloc(header.length + 1);
        if (!payload) break;

        read(sock, payload, header.length);
        payload[header.length] = '\0';

        printf("[Ricevuto] Offset: %lu -> Contenuto: %s\n", header.offset, payload);
        free(payload);
        uint64_t next_offset = header.offset + 1;
        uint8_t commit = CMD_COMMIT;
        write(sock, &topic_len, 1);
        write(sock, topic, topic_len);
        write(sock, &partition, sizeof(uint32_t));
        write(sock, &commit, sizeof(uint8_t));
    }

    close(sock);
    return 0;
}
