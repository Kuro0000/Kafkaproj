#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdint.h>
#include <signal.h>


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
    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IOLBF, 0);
    char *host=argv[1];
    char *port_str=argv[2];
    char *topic =argv[3];
    uint32_t partition = (uint32_t)strtoul(argv[4], NULL, 10);
    uint64_t offset = strtoull(argv[5], NULL, 10);
    int poll_interval = (argc > 6) ? atoi(argv[6]) : 2;
    struct addrinfo hints, *res;
    int sock;
    for(;;){

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        if (getaddrinfo(host, port_str, &hints, &res) != 0) {
            perror("Risoluzione host fallita");
            continue;
        }
        sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sock < 0) {
            freeaddrinfo(res);
            perror("Creazione socket fallita");
            continue;
        }

        if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
            close(sock);
            freeaddrinfo(res);
            perror("Connessione al broker fallita");
            continue;
        }
        freeaddrinfo(res);

        uint8_t opcode_fetch = CMD_FETCH;
        uint8_t topic_len = strlen(topic);
        write(sock, &opcode_fetch, 1);
        write(sock, &topic_len, 1);
        write(sock, topic, topic_len);
        write(sock, &partition, sizeof(uint32_t));
        write(sock, &offset, sizeof(uint64_t));
        int ricevuti = 0;
        MessageHeader header;

        while(read(sock, &header, sizeof(MessageHeader)) == sizeof(MessageHeader)) {
            char *payload = malloc(header.length + 1);
            if (!payload) break;
            uint32_t remaining = header.length;
            char *ptr = payload;
            while (remaining > 0) {
                ssize_t n = read(sock, ptr, remaining);
                if (n <= 0) {
                    free(payload);
                    break;
                }
                remaining -= n;
                ptr += n;
            }
            payload[header.length] = '\0';
            printf("[Ricevuto] Offset: %llu -> Contenuto: %s\n", 
                   (unsigned long long)header.offset, payload);
            free(payload);

            offset = header.offset + 1;
            ricevuti++;
        }
        close(sock);
        if(ricevuti>0){
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            if (getaddrinfo(host, port_str, &hints, &res) == 0) {
                int sock_commit = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
                if (sock_commit >= 0) {
                    if (connect(sock_commit, res->ai_addr, res->ai_addrlen) == 0) {
                        uint8_t opcode_commit = CMD_COMMIT;
                        write(sock_commit, &opcode_commit, 1);
                        write(sock_commit, &topic_len, 1);
                        write(sock_commit, topic, topic_len);
                        write(sock_commit, &partition, sizeof(uint32_t));
                        write(sock_commit, &offset, sizeof(uint64_t));
                        uint8_t ack = 0;
                        if (read(sock_commit, &ack, 1) == 1 && ack == 1) {
                            printf("[Commit] salvato offset %llu (%d messaggi in batch)\n",
                                   (unsigned long long)offset, ricevuti);
                        } else {
                            fprintf(stderr, "Avviso: commit fallito o ack mancante.\n");
                        }
                    } else {
                        fprintf(stderr, "Avviso: impossibile connettersi per il commit.\n");
                    }
                    close(sock_commit);
                }
                freeaddrinfo(res);
            }
        }
        fflush(stdout);
        sleep(poll_interval);
    }

    close(sock);
    return 0;
}
