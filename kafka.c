#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <pthread.h>
#include <netdb.h>

#define PORT 9092
#define CMD_PRODUCE 0x01
#define CMD_FETCH   0x02
#define INDEX_INTERVAL 3 
#define MAX_PARTITIONS 100
#define PARTITIONS_PER_TOPIC 5

typedef struct {
    uint64_t offset;
    uint32_t length;
} MessageHeader;

typedef struct {
    uint64_t offset;
    uint64_t pos;
} IndexEntry;


typedef struct {
    char topic[64];
    int id;
    uint64_t next;
    pthread_mutex_t lock;
} Partition;


Partition partitions[MAX_PARTITIONS];
int partition_count = 0;
pthread_mutex_t partition_manager_lock = PTHREAD_MUTEX_INITIALIZER;


// posizione tramite ricerca binaria
uint64_t find_closest_byte_position(const char *path, uint64_t target){
    FILE *f = fopen(path, "rb");
    if (!f)
        return 0;
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    int l = 0;
    int r =size/sizeof(IndexEntry)-1;

    uint64_t pos = 0;
    while(l <= r)
    {
        int m = l+(r-l)/2;

        fseek(f, m * sizeof(IndexEntry), SEEK_SET);
        IndexEntry e;
        if (fread(&e, sizeof(e), 1, f) != 1)
            break;

        if (e.offset <= target){
            pos = e.pos;
            l =m+1;
        }
        else{
            r = m-1;
        }
    }

    fclose(f);

    return pos;
}

//gestisce la pratizione
Partition *get_partition(const char *topic, int id){
    if (id < 0 || id >= PARTITIONS_PER_TOPIC)
        return NULL;

        pthread_mutex_lock(&partition_manager_lock);

    for(int i = 0; i < partition_count; i++)
    {
        if(partitions[i].id == id && strcmp(partitions[i].topic, topic) == 0)
        {
            pthread_mutex_unlock(&partition_manager_lock);
            return &partitions[i];
        }
    }


    if(partition_count >= MAX_PARTITIONS)
    {
        pthread_mutex_unlock(&partition_manager_lock);
        return NULL;
    }

    Partition *p = &partitions[partition_count];

    memset(p, 0, sizeof(Partition));

    strncpy(p->topic, topic, sizeof(p->topic)-1);

    p->id = id;
    p->next  = 0;

    pthread_mutex_init(&p->lock, NULL);

    char index_file_path[256];
    char log_file_path[256];
    snprintf(index_file_path, sizeof(index_file_path), "data/%s/partition-%d/index.bin", topic, id);
    snprintf(log_file_path, sizeof(log_file_path), "data/%s/partition-%d/log.bin", topic, id);

    uint64_t offset = 0;

    FILE *log_read = fopen(log_file_path, "rb");

    if (log_read) {

        MessageHeader h;

        while(fread(&h, sizeof(MessageHeader), 1, log_read) == 1) {

            offset = h.offset + 1;

            fseek(log_read, h.length, SEEK_CUR);
        }

        fclose(log_read);
    }
    p->next  = offset;

    partition_count++;

    pthread_mutex_unlock(&partition_manager_lock);


    return p;
}



// Crea cartelle e scrive log e indice
int64_t append_message_to_topic(const char *topic,int id, const char *payload) {


    if(mkdir("data", 0755) == -1 && errno != EEXIST) {
        perror("Errore nella creazione della cartella 'data'");

        return -1;
    }
    char topic_path[256];
    snprintf(topic_path, sizeof(topic_path), "data/%s", topic);
    if(mkdir(topic_path, 0755) == -1 && errno != EEXIST) {
        perror("Errore nella creazione della cartella del topic");
        return -1;
    }
    char partition_path[256];
    snprintf(partition_path, sizeof(partition_path), "%s/partition-%d", topic_path, id);
    if(mkdir(partition_path, 0755) == -1 && errno != EEXIST) {
        perror("Errore nella creazione della cartella della partizione");
        return -1;
    }
    char log_file_path[256];
    snprintf(log_file_path, sizeof(log_file_path), "%s/log.bin", partition_path);
    char index_file_path[256];
    snprintf(index_file_path, sizeof(index_file_path), "%s/index.bin", partition_path);
    Partition *partition = get_partition(topic, id);

    if(partition == NULL)
     return -1;


    pthread_mutex_lock(&partition->lock);
    uint64_t prossimo_id = partition->next;

    FILE *log = fopen(log_file_path, "ab");
    if (!log) {
        pthread_mutex_unlock(&partition->lock);
        return -1;
    }

    fseek(log, 0, SEEK_END);
    long posizione_byte_attuale = ftell(log);

    uint32_t len = strlen(payload);
    MessageHeader header = { 
        .offset = prossimo_id, 
        .length = len };
    
    fwrite(&header, sizeof(MessageHeader), 1, log);
    fwrite(payload, sizeof(char), len, log);
    fclose(log);
    partition->next++;
    // aggiornamento dell'indice
    if(prossimo_id % INDEX_INTERVAL == 0) {
        FILE *idx_file = fopen(index_file_path, "ab");
        if(idx_file){
            IndexEntry nuova_voce = {
                .offset = prossimo_id,
                .pos = posizione_byte_attuale
            };
            fwrite(&nuova_voce, sizeof(IndexEntry), 1, idx_file);
            fclose(idx_file);
        }
    }
pthread_mutex_unlock(&partition->lock);
    return prossimo_id;
}

int forward_to_replica(const char *topic,int id, const char *payload) {
    char *replica_host = getenv("REPLICA_HOST");
    char *replica_port_env = getenv("REPLICA_PORT");
    
    if (!replica_host || !replica_port_env) {
        return 0; 
    }

    struct addrinfo hints;
    struct addrinfo *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;

    if (getaddrinfo(replica_host, replica_port_env, &hints, &res) != 0) {
        return -1;
    }

    int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock < 0) {
        freeaddrinfo(res);
        return -1;
    }

    if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
        close(sock);
        freeaddrinfo(res);
        return -1;
    }
    freeaddrinfo(res);

    uint8_t opcode = CMD_PRODUCE;
    write(sock, &opcode, 1);

    uint8_t topic_len = strlen(topic);
    write(sock, &topic_len, 1);
    write(sock, topic, topic_len);
    
    uint32_t part = (uint32_t)id;
    write(sock, &part, sizeof(uint32_t));
    uint32_t payload_len = strlen(payload);
    write(sock, &payload_len, sizeof(uint32_t));
    write(sock, payload, payload_len);

    int64_t replica_offset = -1;
    ssize_t n = read(sock, &replica_offset, sizeof(int64_t));
    close(sock);

    return (n == sizeof(int64_t) && replica_offset >= 0) ? 0 : -1;
}

// Gestisce il comando produce
void handle_produce(int client_fd, const char *topic) {
    uint32_t id = 0;
    if(read(client_fd, &id, sizeof(uint32_t)) != sizeof(uint32_t))
        return;
    if(id >= PARTITIONS_PER_TOPIC)
        return;
    uint32_t length = 0;
    if(read(client_fd, &length, sizeof(uint32_t)) != sizeof(uint32_t)) //lunghezza payload
        return;
    char *buffer = malloc(length + 1);
    if(!buffer) 
    return;

    uint32_t remaining = length;
    char *ptr = buffer;
    while(remaining > 0) {
        ssize_t n = read(client_fd, ptr, remaining);
        if (n <= 0) { 
            free(buffer); 
            return; 
        }
        remaining -= n; ptr += n;
    }
    buffer[length] = '\0'; 

    int64_t offset = append_message_to_topic(topic, id, buffer);
    if(offset >= 0) {
        if (forward_to_replica(topic, id, buffer) < 0) {
            fprintf(stderr, "Avviso: Impossibile replicare il messaggio sul nodo secondario.\n");
        }

        write(client_fd, &offset, sizeof(int64_t));
    }
    free(buffer);
}

// Gestisce il comando fetch 
void handle_fetch(int client_fd, const char *topic) {
        uint32_t id = 0;
    if (read(client_fd, &id, sizeof(uint32_t)) != sizeof(uint32_t)) {
        return;
    }
    if (id >= PARTITIONS_PER_TOPIC) {
        return;
    }
    uint64_t client_target_offset = 0;
    
    if (read(client_fd, &client_target_offset, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return;
    }
        Partition *partition = get_partition(topic, id);
        if (partition == NULL) {
            return;
        }
    char log_file_path[256];
    snprintf(log_file_path, sizeof(log_file_path), "data/%s/partition-%d/log.bin", topic, id);
    char index_file_path[256];
    snprintf(index_file_path, sizeof(index_file_path), "data/%s/partition-%d/index.bin", topic, id);

    uint64_t byte_di_partenza = find_closest_byte_position(index_file_path, client_target_offset);

    FILE *log = fopen(log_file_path, "rb");
    if (!log) {
        return;
    }
    
    fseek(log, byte_di_partenza, SEEK_SET);
    
    MessageHeader header;
    while(fread(&header, sizeof(MessageHeader), 1, log) == 1) {
        if (header.offset >= client_target_offset) {
            char *payload = malloc(header.length);
            if (!payload) {
                fclose(log);
                return;
            }
            fread(payload, sizeof(char), header.length, log);
            
            write(client_fd, &header, sizeof(MessageHeader));
            write(client_fd, payload, header.length);
            
            free(payload);
        } else {
            fseek(log, header.length, SEEK_CUR);
        }
    }
    fclose(log);
}

void *clientHandler(void *arg) {
        int client_fd = *(int *)arg;
    free(arg);
            uint8_t opcode = 0;
        if (read(client_fd, &opcode, 1) > 0) {   
            uint8_t topic_len = 0;
        if (read(client_fd, &topic_len, 1) > 0) {
                
            char *topic_name = malloc(topic_len + 1);
                if (read(client_fd, topic_name, topic_len) == topic_len) {
                    topic_name[topic_len] = '\0'; 
                    
                    if (opcode == CMD_PRODUCE) {
                        printf("-> PRODUCE sul topic: '%s'\n", topic_name);
                        handle_produce(client_fd, topic_name);
                    } 
                    else if (opcode == CMD_FETCH) {
                        printf("-> FETCH dal topic: '%s'\n", topic_name);
                        handle_fetch(client_fd, topic_name);
                    }
                }
                free(topic_name);
            }
        }
        close(client_fd);
        return NULL;
}


int main() {
    int server_fd, client_fd;
    struct sockaddr_in address;
    int opt=1;
    socklen_t addrlen =sizeof(address);

    int server_port = PORT;
    char *env_port = getenv("PORT");
    if (env_port != NULL) {
        server_port = atoi(env_port);
    }

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) 
        exit(EXIT_FAILURE);
    
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) 
        exit(EXIT_FAILURE);

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(server_port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address))<0) 
    exit(EXIT_FAILURE);
    
    if (listen(server_fd, 3) < 0) exit(EXIT_FAILURE);

    printf("Server Multi-Topic in ascolto sulla porta %d...\n\n", server_port);
    //thread per i client
        pthread_t thread;

    for(;;) {
        if ((client_fd =accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen))< 0) 
            continue;
        int *fd = malloc(sizeof(int));
        if (!fd) {
            close(client_fd);
            continue;
        }

        *fd = client_fd;

        pthread_create(&thread, NULL, clientHandler, fd);
        pthread_detach(thread);
    }
    close(server_fd);
    return 0;
}