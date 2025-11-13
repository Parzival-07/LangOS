#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>
#include <signal.h>

// --- CONFIGURATION ---
#define MAX_CONNECTIONS 1024 // Max clients + storage servers

// --- INTERNAL DATA STRUCTURES ---

typedef enum {
    CONN_TYPE_FREE = 0,
    CONN_TYPE_SS = 1,
    CONN_TYPE_CLIENT = 2
} ConnectionType;

// Info the NM keeps about each connection
typedef struct {
    int socket;
    ConnectionType type;
    char ip_addr[INET_ADDRSTRLEN];
    
    // Client-specific data
    char username[MAX_USERNAME_LEN];

    // SS-specific data
    int client_port; // Port clients should use to connect to this SS
    // Serialize NM<->SS request/response to avoid interleaved replies
    pthread_mutex_t ss_io_mutex;
    
} ConnectionInfo;

// This struct is passed to each new thread
typedef struct {
    int socket;
    char ip_addr[INET_ADDRSTRLEN];
} ThreadArgs;

// --- GLOBAL STATE ---
// This array is the "brain" of the Name Server.
// We use a simple array instead of a complex data structure.
ConnectionInfo connections[MAX_CONNECTIONS];
pthread_mutex_t connections_mutex = PTHREAD_MUTEX_INITIALIZER;

// --- FILE REGISTRY (maintained by NM) ---
#define MAX_FILES 10000
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char owner[MAX_USERNAME_LEN];
    int ss_slot; // which storage server holds this file
    // Cached metadata for INFO/VIEW (populated from SS during refresh)
    long long created;
    long long updated;
    long long size_bytes;
    long long words_cnt;
    long long chars_cnt;
    long long last_access;
    long long last_modified;
    char readers[1024];
    char writers[1024];
    char info_str[2048]; // raw INFO payload for direct responses
} FileRecord;

static FileRecord file_registry[MAX_FILES];
static int file_count = 0;
static pthread_mutex_t file_registry_mutex = PTHREAD_MUTEX_INITIALIZER;

// --- FAST LOOKUP INDEX (O(1) average) ---
// Hash map from filename -> index in file_registry[] using open hashing (separate chaining).
#define FILE_INDEX_BUCKETS 4096
typedef struct FileIndexNode {
    char key[MAX_FILENAME_LEN];
    int idx; // index into file_registry
    struct FileIndexNode* next;
} FileIndexNode;

static FileIndexNode* file_index[FILE_INDEX_BUCKETS];
static pthread_mutex_t file_index_mutex = PTHREAD_MUTEX_INITIALIZER;

static unsigned long fnv1a_hash(const char* s) {
    unsigned long h = 1469598103934665603ULL; // 64-bit FNV offset basis
    while (*s) {
        h ^= (unsigned char)(*s++);
        h *= 1099511628211ULL; // 64-bit FNV prime
    }
    return (unsigned long)h;
}

static inline int bucket_for(const char* key) {
    return (int)(fnv1a_hash(key) & (FILE_INDEX_BUCKETS - 1)); // buckets is power of two
}

static void file_index_clear_unlocked(void) {
    for (int i = 0; i < FILE_INDEX_BUCKETS; i++) {
        FileIndexNode* n = file_index[i];
        while (n) { FileIndexNode* nx = n->next; free(n); n = nx; }
        file_index[i] = NULL;
    }
}

static void file_index_build(void) {
    pthread_mutex_lock(&file_index_mutex);
    file_index_clear_unlocked();
    pthread_mutex_lock(&file_registry_mutex);
    for (int i = 0; i < file_count; i++) {
        const char* k = file_registry[i].filename;
        if (!k[0]) continue;
        int b = bucket_for(k);
        FileIndexNode* n = (FileIndexNode*)calloc(1, sizeof(FileIndexNode));
        if (!n) continue;
        strncpy(n->key, k, sizeof(n->key)-1);
        n->idx = i;
        n->next = file_index[b];
        file_index[b] = n;
    }
    pthread_mutex_unlock(&file_registry_mutex);
    pthread_mutex_unlock(&file_index_mutex);
}

static void file_index_insert(const char* filename, int idx) {
    if (!filename || !*filename || idx < 0) return;
    pthread_mutex_lock(&file_index_mutex);
    int b = bucket_for(filename);
    for (FileIndexNode* n = file_index[b]; n; n = n->next) {
        if (strncmp(n->key, filename, MAX_FILENAME_LEN) == 0) { n->idx = idx; pthread_mutex_unlock(&file_index_mutex); return; }
    }
    FileIndexNode* n = (FileIndexNode*)calloc(1, sizeof(FileIndexNode));
    if (n) {
        strncpy(n->key, filename, sizeof(n->key)-1);
        n->idx = idx;
        n->next = file_index[b];
        file_index[b] = n;
    }
    pthread_mutex_unlock(&file_index_mutex);
}

static int file_index_find(const char* filename) {
    if (!filename || !*filename) return -1;
    int b = bucket_for(filename);
    pthread_mutex_lock(&file_index_mutex);
    for (FileIndexNode* n = file_index[b]; n; n = n->next) {
        if (strncmp(n->key, filename, MAX_FILENAME_LEN) == 0) {
            int idx = n->idx;
            pthread_mutex_unlock(&file_index_mutex);
            return idx;
        }
    }
    pthread_mutex_unlock(&file_index_mutex);
    return -1;
}

// --- Small helpers ---
// Check if a space-separated list contains a username (exact token match)
static int nm_list_contains(const char* list, const char* user) {
    if (!list || !user || !*user) return 0;
    const unsigned char* p = (const unsigned char*)list;
    // tokens are separated by commas and/or whitespace
    while (*p) {
        // skip separators
        while (*p && (isspace(*p) || *p==',')) p++;
        if (!*p) break;
        char tok[MAX_USERNAME_LEN] = {0};
        size_t k = 0;
        while (*p && !(isspace(*p) || *p==',')) {
            if (k + 1 < sizeof(tok)) tok[k++] = (char)*p;
            p++;
        }
        tok[k] = '\0';
        if (tok[0] && strncmp(tok, user, MAX_USERNAME_LEN) == 0) return 1;
        // continue scanning from current p (which is at a separator)
    }
    return 0;
}

// --- PROTOTYPES ---
void* handle_connection(void* arg);
void handle_register_ss(int slot, const MsgRegisterSS* msg);
void handle_register_client(int slot, const MsgRegisterClient* msg);
void send_ack(int socket);
// Forward declarations for helpers used in handle_connection
static int choose_ss_slot(void);
static void send_error(int socket, int code, const char* msg);
static void registry_add_file_if_absent(const char* filename, const char* owner, int ss_slot);
static void registry_refresh_from_ss(void);
static void registry_update_one_from_ss(int ss_slot, const char* filename);

// Helper: fetch file contents from SS using client port (header-based READ)
static int nm_fetch_file_from_ss(const char* ss_ip, int ss_port, const char* filename, const char* requester, char** out_data, int* out_size) {
    if (!ss_ip || !filename || !requester || !out_data || !out_size) return -1;
    *out_data = NULL; *out_size = 0;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -2;
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons((uint16_t)ss_port);
    if (inet_pton(AF_INET, ss_ip, &addr.sin_addr) <= 0) { close(fd); return -3; }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { close(fd); return -4; }
    MsgHeader rq = (MsgHeader){ .command = CMD_READ_FILE, .payload_size = sizeof(MsgReadFile) };
    MsgReadFile rf = {0}; strncpy(rf.filename, filename, MAX_FILENAME_LEN-1); strncpy(rf.requester, requester, MAX_USERNAME_LEN-1);
    if (!send_all(fd, &rq, sizeof(rq)) || !send_all(fd, &rf, sizeof(rf))) { close(fd); return -5; }
    MsgHeader rs; if (!recv_all(fd, &rs, sizeof(rs))) { close(fd); return -6; }
    if (rs.command == CMD_ERROR && rs.payload_size == sizeof(MsgError)) {
        // consume error payload but signal error by negative size
        MsgError e; (void)recv_all(fd, &e, sizeof(e)); close(fd); return -1000 - e.code; // encode error code
    }
    if (rs.command != CMD_ACK || rs.payload_size < 0) {
        // drain unexpected
        int rem = rs.payload_size; char drain[512]; while (rem > 0) { int ch = rem>(int)sizeof(drain)?(int)sizeof(drain):rem; ssize_t r = recv(fd, drain, (size_t)ch, 0); if (r <= 0) break; rem -= (int)r; }
        close(fd); return -7;
    }
    int n = rs.payload_size;
    char* buf = (char*)malloc((size_t)n + 1);
    if (!buf) { // drain
        int rem = n; char drain[512]; while (rem > 0) { int ch = rem>(int)sizeof(drain)?(int)sizeof(drain):rem; ssize_t r = recv(fd, drain, (size_t)ch, 0); if (r <= 0) break; rem -= (int)r; }
        close(fd); return -8;
    }
    if (n > 0 && !recv_all(fd, buf, (size_t)n)) { free(buf); close(fd); return -9; }
    buf[n] = '\0';
    *out_data = buf; *out_size = n;
    close(fd);
    return 0;
}

// --- MAIN SERVER LOGIC ---

int main() {
    // Ignore SIGPIPE globally so accidental sends to closed sockets don't kill the process
    signal(SIGPIPE, SIG_IGN);

    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    // Initialize connections array
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        connections[i].type = CONN_TYPE_FREE;
        connections[i].socket = -1;
    }

    // Create listening socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Forcefully attach socket to the port (prevents "Address already in use")
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // Listen on all available interfaces
    address.sin_port = htons(NAME_SERVER_PORT);

    // Bind the socket to our port
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Start listening
    if (listen(server_fd, 10) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    LOG_NM("Name Server listening on port %d...\n", NAME_SERVER_PORT);

    // --- MAIN ACCEPT LOOP ---
    while (true) {
        struct sockaddr_in client_address;
        socklen_t addrlen = sizeof(client_address);
        
        int client_socket = accept(server_fd, (struct sockaddr *)&client_address, &addrlen);
        if (client_socket < 0) {
            perror("accept");
            continue; // Keep listening
        }

        // Prepare args for the new thread
        ThreadArgs* args = (ThreadArgs*)malloc(sizeof(ThreadArgs));
        if (!args) {
            perror("malloc failed");
            close(client_socket);
            continue;
        }
        args->socket = client_socket;
        inet_ntop(AF_INET, &client_address.sin_addr, args->ip_addr, INET_ADDRSTRLEN);

    LOG_NM("New connection accepted from %s\n", args->ip_addr);

        // Create a new thread to handle this connection
        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, handle_connection, (void*)args) != 0) {
            perror("pthread_create");
            free(args);
            close(client_socket);
        }
        pthread_detach(thread_id); // We don't need to join it
    }

    close(server_fd);
    pthread_mutex_destroy(&connections_mutex);
    return 0;
}

// --- THREAD FUNCTION ---

/**
 * @brief Thread function to handle a single connection (either SS or Client).
 */
void* handle_connection(void* arg) {
    ThreadArgs* args = (ThreadArgs*)arg;
    int socket = args->socket;
    char ip[INET_ADDRSTRLEN];
    strcpy(ip, args->ip_addr);
    free(args); // We've copied the args, so free the heap memory

    // Find a free slot in the connections array
    int slot = -1;
    pthread_mutex_lock(&connections_mutex);
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (connections[i].type == CONN_TYPE_FREE) {
            slot = i;
            connections[i].type = -1; // Mark as "in progress"
            connections[i].socket = socket;
            strcpy(connections[i].ip_addr, ip);
            break;
        }
    }
    pthread_mutex_unlock(&connections_mutex);

    if (slot == -1) {
        LOGE_NM("Max connections reached. Rejecting %s.\n", ip);
        // TODO: Send CMD_ERROR (Server Busy)
        close(socket);
        return NULL;
    }

    MsgHeader header;
    // Read the first header to see who this is
    if (!recv_all(socket, &header, sizeof(MsgHeader))) {
        LOGE_NM("Failed to read header from %s. Disconnecting.\n", ip);
        close(socket);
        // Free the slot
        pthread_mutex_lock(&connections_mutex);
        connections[slot].type = CONN_TYPE_FREE;
        connections[slot].socket = -1;
        pthread_mutex_unlock(&connections_mutex);
        return NULL;
    }

    // Route based on the command
    bool registration_successful = true;
    switch (header.command) {
        case CMD_REGISTER_SS: {
            MsgRegisterSS msg;
            if (!recv_all(socket, &msg, sizeof(MsgRegisterSS))) {
                LOGE_NM("Failed to read REG_SS payload from %s.\n", ip);
                registration_successful = false;
            } else {
                handle_register_ss(slot, &msg);
            }
            break;
        }
        case CMD_REGISTER_CLIENT: {
            MsgRegisterClient msg;
            if (!recv_all(socket, &msg, sizeof(MsgRegisterClient))) {
                LOGE_NM("Failed to read REG_CLIENT payload from %s.\n", ip);
                registration_successful = false;
            } else {
                handle_register_client(slot, &msg);
            }
            break;
        }
        default:
            LOGE_NM("Unknown command %d from %s.\n", header.command, ip);
            registration_successful = false;
            break;
    }
    
    if (!registration_successful) {
        // Free the slot
        pthread_mutex_lock(&connections_mutex);
        connections[slot].type = CONN_TYPE_FREE;
        connections[slot].socket = -1;
        pthread_mutex_unlock(&connections_mutex);
        close(socket);
        return NULL;
    }
    
    // --- MAIN COMMAND LOOP for this connection ---
    LOG_NM("Connection from %s (Socket %d, Slot %d) registered. Now in idle loop.\n", ip, socket, slot);

    // IMPORTANT: Do not read from SS sockets here, or you'll steal replies
    // that client threads are synchronously waiting to proxy.
    if (connections[slot].type == CONN_TYPE_SS) {
        // Park this thread; keep the socket open for client threads to use.
        while (true) {
            sleep(3600);
        }
    }

    while (recv_all(socket, &header, sizeof(MsgHeader))) {
        LOG_NM("Received command %d from socket %d (Slot %d)\n", header.command, socket, slot);

        // Handle selected commands here
        if (header.command == CMD_CREATE_FILE && connections[slot].type == CONN_TYPE_CLIENT) {
            if (header.payload_size != sizeof(MsgCreateFile)) {
                send_error(socket, 400, "Bad payload size");
                continue;
            }
            MsgCreateFile payload;
            if (!recv_all(socket, &payload, sizeof(payload))) {
                send_error(socket, 400, "Failed to read payload");
                continue;
            }

            int ss_slot = choose_ss_slot();
            if (ss_slot < 0) {
                LOGE_NM("No Storage Server available for CREATE '%s'\n", payload.filename);
                send_error(socket, 503, "No Storage Server available");
                continue;
            }

            // Forward to SS with per-SS serialization
            int ss_sock;
            pthread_mutex_lock(&connections_mutex);
            ss_sock = connections[ss_slot].socket;
            pthread_mutex_unlock(&connections_mutex);
         LOG_NM("Forwarding CREATE '%s' (owner=%s) to SS at %s:%d (Slot %d)\n",
             payload.filename, payload.owner, connections[ss_slot].ip_addr, connections[ss_slot].client_port, ss_slot);

            pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
            MsgHeader ss_resp;
            do {
                MsgHeader fwd = {0};
                fwd.command = CMD_CREATE_FILE;
                fwd.payload_size = sizeof(MsgCreateFile);
                if (!send_all(ss_sock, &fwd, sizeof(fwd)) ||
                    !send_all(ss_sock, &payload, sizeof(payload))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "Failed to contact Storage Server");
                    continue;
                }

                // Receive SS response here and proxy it back to the client
                if (!recv_all(ss_sock, &ss_resp, sizeof(ss_resp))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "No response from Storage Server");
                    continue;
                }
                // If SS acknowledged creation, update NM registry before proxying
                if (ss_resp.command == CMD_ACK) {
                    registry_add_file_if_absent(payload.filename, payload.owner, ss_slot);
                }

                // Send header to client
                if (!send_all(socket, &ss_resp, sizeof(ss_resp))) {
                    // Client disconnected
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    continue;
                }

                // Forward payload in full (if any) while holding SS lock
                size_t remaining = ss_resp.payload_size;
                char buf[4096];
                while (remaining > 0) {
                    size_t chunk = remaining > sizeof(buf) ? sizeof(buf) : remaining;
                    if (!recv_all(ss_sock, buf, chunk)) {
                        pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                        send_error(socket, 502, "Failed to read SS payload");
                        break;
                    }
                    if (!send_all(socket, buf, chunk)) {
                        break;
                    }
                    remaining -= chunk;
                }
            } while (0);
            pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
            // Refresh NM cache for this file so subsequent INFO/VIEW are up-to-date
            registry_update_one_from_ss(ss_slot, payload.filename);
            continue;
        } else if (header.command == CMD_VIEW_FILES && connections[slot].type == CONN_TYPE_CLIENT) {
            // Serve from cache; only refresh if user asked for all files and cache is empty

            // Validate payload
            if (header.payload_size != sizeof(MsgViewFilesRequest)) {
                size_t rem = header.payload_size; char drain[512];
                while (rem > 0) { size_t chunk = rem > sizeof(drain)?sizeof(drain):rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
                send_error(socket, 400, "Bad payload size");
                continue;
            }
            MsgViewFilesRequest vreq = {0};
            if (!recv_all(socket, &vreq, sizeof(vreq))) { send_error(socket, 400, "Payload read failed"); continue; }

            if (vreq.show_all) {
                int fc = 0;
                pthread_mutex_lock(&file_registry_mutex);
                fc = file_count;
                pthread_mutex_unlock(&file_registry_mutex);
                if (fc == 0) {
                    registry_refresh_from_ss();
                }
            }

            // Use authenticated requester; honor flags:
            // - show_all=0: list only files requester can access (owner/readers/writers)
            // - show_all=1: list all files in the system
            char requester[MAX_USERNAME_LEN] = {0};
            strncpy(requester, connections[slot].username, MAX_USERNAME_LEN-1);

            char buffer[16384]; buffer[0] = '\0'; size_t pos = 0, cap = sizeof(buffer);

            // If long_list is requested, gather targets and refresh INFO per file for dynamic stats
            if (vreq.long_list) {
                // Snapshot current files to consider (name + ss_slot) honoring access filter
                typedef struct { char name[MAX_FILENAME_LEN]; int ss_slot; } ToRefresh;
                ToRefresh tmp[1024]; int n_tmp = 0;
                pthread_mutex_lock(&file_registry_mutex);
                for (int i = 0; i < file_count && n_tmp < (int)(sizeof(tmp)/sizeof(tmp[0])); i++) {
                    const char* fname = file_registry[i].filename;
                    const char* owner = file_registry[i].owner;
                    const char* readers = file_registry[i].readers;
                    const char* writers = file_registry[i].writers;
                    int include_file = 1;
                    if (!vreq.show_all) {
                        include_file = 0;
                        if (owner[0] && strncmp(owner, requester, MAX_USERNAME_LEN)==0) include_file = 1;
                        else if (nm_list_contains(readers, requester)) include_file = 1;
                        else if (nm_list_contains(writers, requester)) include_file = 1;
                    }
                    if (!include_file) continue;
                    strncpy(tmp[n_tmp].name, fname, MAX_FILENAME_LEN-1); tmp[n_tmp].name[MAX_FILENAME_LEN-1]='\0';
                    tmp[n_tmp].ss_slot = file_registry[i].ss_slot;
                    n_tmp++;
                }
                pthread_mutex_unlock(&file_registry_mutex);
                // Refresh each selected file from SS for dynamic stats
                for (int i = 0; i < n_tmp; i++) {
                    registry_update_one_from_ss(tmp[i].ss_slot, tmp[i].name);
                }
            }

            pthread_mutex_lock(&file_registry_mutex);
            for (int i = 0; i < file_count; i++) {
                const char* fname = file_registry[i].filename;
                const char* owner = file_registry[i].owner;
                const char* readers = file_registry[i].readers;
                const char* writers = file_registry[i].writers;
                char linebuf[2048] = {0};

                int include_file = 1;
                if (!vreq.show_all) {
                    include_file = 0;
                    if (owner[0] && strncmp(owner, requester, MAX_USERNAME_LEN)==0) include_file = 1;
                    else if (nm_list_contains(readers, requester)) include_file = 1;
                    else if (nm_list_contains(writers, requester)) include_file = 1;
                }
                if (!include_file) continue;

                if (vreq.long_list) {
                    // Build compact one-line from cached fields
                    char last[128] = "", lastmod[128] = "";
                    time_t ac = (time_t)file_registry[i].last_access;
                    time_t mo = (time_t)file_registry[i].last_modified;
                    struct tm tmv;
                    if (file_registry[i].last_access && localtime_r(&ac, &tmv)) strftime(last, sizeof(last), "%Y-%m-%d %H:%M:%S", &tmv);
                    if (file_registry[i].last_modified && localtime_r(&mo, &tmv)) strftime(lastmod, sizeof(lastmod), "%Y-%m-%d %H:%M:%S", &tmv);
                    snprintf(linebuf, sizeof(linebuf), "%s\towner:%s\tsize:%lld\twords:%lld\tchars:%lld\tlast_access:%s (%lld)\tlast_modified:%s (%lld)\n",
                             fname,
                             owner[0]?owner:"",
                             file_registry[i].size_bytes,
                             file_registry[i].words_cnt,
                             file_registry[i].chars_cnt,
                             last[0]?last:"",
                             file_registry[i].last_access,
                             lastmod[0]?lastmod:"",
                             file_registry[i].last_modified);
                } else {
                    snprintf(linebuf, sizeof(linebuf), "%s\n", fname);
                }

                size_t ln = strlen(linebuf);
                if (pos + ln >= cap) break;
                memcpy(buffer + pos, linebuf, ln); pos += ln; buffer[pos] = '\0';
            }
            pthread_mutex_unlock(&file_registry_mutex);

            MsgViewFilesResponse resp = {0};
            strncpy(resp.file_list, buffer, sizeof(resp.file_list)-1);
            MsgHeader h = { .command = CMD_VIEW_FILES_RESP, .payload_size = sizeof(resp) };
            send_all(socket, &h, sizeof(h));
            send_all(socket, &resp, sizeof(resp));
            continue;
        } else if (header.command == CMD_READ_FILE && connections[slot].type == CONN_TYPE_CLIENT) {
            if (header.payload_size != sizeof(MsgReadFile)) {
                send_error(socket, 400, "Bad payload size");
                // Drain unexpected payload if any
                size_t rem = header.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
                continue;
            }
            MsgReadFile req;
            if (!recv_all(socket, &req, sizeof(req))) {
                send_error(socket, 400, "Payload read failed");
                continue;
            }

            // Try cache-first; refresh only on miss

            MsgReadFileResponse resp = {0};
            resp.found = 0;

            int idx = file_index_find(req.filename);
            pthread_mutex_lock(&file_registry_mutex);
            if (idx >= 0) {
                int ss_slot = file_registry[idx].ss_slot;
                pthread_mutex_lock(&connections_mutex);
                if (ss_slot >= 0 && ss_slot < MAX_CONNECTIONS && connections[ss_slot].type == CONN_TYPE_SS && connections[ss_slot].socket >= 0) {
                    resp.found = 1;
                    strncpy(resp.ss_ip, connections[ss_slot].ip_addr, MAX_IP_LEN - 1);
                    resp.ss_port = connections[ss_slot].client_port;
                }
                pthread_mutex_unlock(&connections_mutex);
            }
            pthread_mutex_unlock(&file_registry_mutex);

            if (!resp.found) {
                registry_refresh_from_ss();
                idx = file_index_find(req.filename);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx >= 0) {
                    int ss_slot = file_registry[idx].ss_slot;
                    pthread_mutex_lock(&connections_mutex);
                    if (ss_slot >= 0 && ss_slot < MAX_CONNECTIONS && connections[ss_slot].type == CONN_TYPE_SS && connections[ss_slot].socket >= 0) {
                        resp.found = 1;
                        strncpy(resp.ss_ip, connections[ss_slot].ip_addr, MAX_IP_LEN - 1);
                        resp.ss_port = connections[ss_slot].client_port;
                    }
                    pthread_mutex_unlock(&connections_mutex);
                }
                pthread_mutex_unlock(&file_registry_mutex);
            }

            MsgHeader h = {0};
            h.command = CMD_READ_FILE_RESP;
            h.payload_size = sizeof(resp);
            send_all(socket, &h, sizeof(h));
            send_all(socket, &resp, sizeof(resp));
            continue;
        } else if ((header.command == CMD_ADD_ACCESS || header.command == CMD_REM_ACCESS) && connections[slot].type == CONN_TYPE_CLIENT) {
            if (header.payload_size != sizeof(MsgAccessChange)) {
                send_error(socket, 400, "Bad payload size");
                // Drain unexpected payload
                size_t rem = header.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
                continue;
            }
            MsgAccessChange req;
            if (!recv_all(socket, &req, sizeof(req))) {
                send_error(socket, 400, "Payload read failed");
                continue;
            }
            // Find owning SS (cache-first; refresh on miss)
            int ss_slot = -1;
            int idx_acc = file_index_find(req.filename);
            pthread_mutex_lock(&file_registry_mutex);
            if (idx_acc >= 0) ss_slot = file_registry[idx_acc].ss_slot;
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) {
                registry_refresh_from_ss();
                idx_acc = file_index_find(req.filename);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx_acc >= 0) ss_slot = file_registry[idx_acc].ss_slot;
                pthread_mutex_unlock(&file_registry_mutex);
            }
            if (ss_slot < 0) {
                send_error(socket, 404, "File not found");
                continue;
            }

            // Overwrite requester with authenticated client username
            pthread_mutex_lock(&connections_mutex);
            strncpy(req.requester, connections[slot].username, MAX_USERNAME_LEN - 1);
            req.requester[MAX_USERNAME_LEN - 1] = '\0';
            int ss_sock_local = connections[ss_slot].socket;
            pthread_mutex_unlock(&connections_mutex);

            // Forward to SS and proxy response with serialization
            pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
            MsgHeader rs;
            do {
                MsgHeader fwd = {0};
                fwd.command = header.command;
                fwd.payload_size = sizeof(req);
                if (!send_all(ss_sock_local, &fwd, sizeof(fwd)) || !send_all(ss_sock_local, &req, sizeof(req))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "Failed to contact Storage Server");
                    continue;
                }
                if (!recv_all(ss_sock_local, &rs, sizeof(rs))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "No response from Storage Server");
                    continue;
                }
                if (!send_all(socket, &rs, sizeof(rs))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    continue;
                }
                // Proxy any payload while holding SS lock
                size_t rem = rs.payload_size; char buf[512];
                while (rem > 0) {
                    size_t chunk = rem > sizeof(buf) ? sizeof(buf) : rem;
                    if (!recv_all(ss_sock_local, buf, chunk)) break;
                    if (!send_all(socket, buf, chunk)) break;
                    rem -= chunk;
                }
            } while (0);
            pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
            // If ACL change succeeded, refresh cached INFO for this file
            if (rs.command == CMD_ACK) {
                registry_update_one_from_ss(ss_slot, req.filename);
            }
            continue;
        } else if (header.command == CMD_INFO && connections[slot].type == CONN_TYPE_CLIENT) {
            if (header.payload_size != sizeof(MsgInfoRequest)) {
                send_error(socket, 400, "Bad payload size");
                // Drain
                size_t rem = header.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
                continue;
            }
            MsgInfoRequest req;
            if (!recv_all(socket, &req, sizeof(req))) {
                send_error(socket, 400, "Payload read failed");
                continue;
            }

            // Authenticated requester
            char requester[MAX_USERNAME_LEN] = {0};
            strncpy(requester, connections[slot].username, MAX_USERNAME_LEN-1);

            // Ensure registry has mapping; refresh if empty or filename not yet present
            int idx = file_index_find(req.filename);
            int ss_slot = -1;
            pthread_mutex_lock(&file_registry_mutex);
            if (idx >= 0) ss_slot = file_registry[idx].ss_slot;
            pthread_mutex_unlock(&file_registry_mutex);
            if (idx < 0) {
                // Try global refresh to discover file location
                registry_refresh_from_ss();
                idx = file_index_find(req.filename);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx >= 0) ss_slot = file_registry[idx].ss_slot;
                pthread_mutex_unlock(&file_registry_mutex);
            }
            if (idx < 0) { send_error(socket, 404, "File not found"); continue; }

            // If ACL fields or info cache look empty, refresh this one file from its SS
            int need_single_refresh = 0;
            pthread_mutex_lock(&file_registry_mutex);
            if (file_registry[idx].info_str[0] == '\0' || file_registry[idx].owner[0] == '\0') need_single_refresh = 1;
            pthread_mutex_unlock(&file_registry_mutex);
            if (need_single_refresh && ss_slot >= 0) {
                registry_update_one_from_ss(ss_slot, req.filename);
            }

            // Check ACL: owner, reader, or writer only
            int allowed = 0;
            char owner[MAX_USERNAME_LEN] = {0};
            char readers[1024] = {0};
            char writers[1024] = {0};
            pthread_mutex_lock(&file_registry_mutex);
            if (idx >= 0 && idx < file_count) {
                strncpy(owner, file_registry[idx].owner, sizeof(owner)-1);
                strncpy(readers, file_registry[idx].readers, sizeof(readers)-1);
                strncpy(writers, file_registry[idx].writers, sizeof(writers)-1);
            }
            pthread_mutex_unlock(&file_registry_mutex);
            if ((owner[0] && strncmp(owner, requester, MAX_USERNAME_LEN)==0) || nm_list_contains(readers, requester) || nm_list_contains(writers, requester)) {
                allowed = 1;
            }
            if (!allowed) {
                LOG_NM("INFO '%s' denied for user %s\n", req.filename, requester);
                send_error(socket, 403, "Forbidden");
                continue;
            }

            // Always refresh this file's INFO from its owning SS so stats are up-to-date
            if (ss_slot >= 0) {
                registry_update_one_from_ss(ss_slot, req.filename);
            }

            // Serve INFO from NM cache after refresh
            MsgInfoResponse resp = {0};
            pthread_mutex_lock(&file_registry_mutex);
            if (idx >= 0 && idx < file_count) {
                if (file_registry[idx].info_str[0]) {
                    strncpy(resp.info, file_registry[idx].info_str, sizeof(resp.info)-1);
                } else {
                    // Fallback: synthesize minimal info if string missing
                    snprintf(resp.info, sizeof(resp.info),
                             "filename: %s\nowner: %s\nsize: %lld\nwords: %lld\nchars: %lld\n",
                             file_registry[idx].filename,
                             file_registry[idx].owner,
                             file_registry[idx].size_bytes,
                             file_registry[idx].words_cnt,
                             file_registry[idx].chars_cnt);
                }
            }
            pthread_mutex_unlock(&file_registry_mutex);

            MsgHeader h = { .command = CMD_INFO_RESP, .payload_size = sizeof(resp) };
            send_all(socket, &h, sizeof(h));
            send_all(socket, &resp, sizeof(resp));
            continue;
    } else if (header.command == CMD_UNDO && connections[slot].type == CONN_TYPE_CLIENT) {
            if (header.payload_size != sizeof(MsgUndoRequest)) {
                send_error(socket, 400, "Bad payload size");
                // Drain unexpected payload
                size_t rem = header.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
                continue;
            }
            MsgUndoRequest req;
            if (!recv_all(socket, &req, sizeof(req))) { send_error(socket, 400, "Payload read failed"); continue; }
            // Overwrite requester with authenticated username
            strncpy(req.requester, connections[slot].username, MAX_USERNAME_LEN-1);
            req.requester[MAX_USERNAME_LEN-1] = '\0';

            // Find owning SS (cache-first; refresh on miss)
            int ss_slot = -1;
            int idx_undo = file_index_find(req.filename);
            pthread_mutex_lock(&file_registry_mutex);
            if (idx_undo >= 0) ss_slot = file_registry[idx_undo].ss_slot;
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) {
                registry_refresh_from_ss();
                idx_undo = file_index_find(req.filename);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx_undo >= 0) ss_slot = file_registry[idx_undo].ss_slot;
                pthread_mutex_unlock(&file_registry_mutex);
            }
            if (ss_slot < 0) { send_error(socket, 404, "File not found"); continue; }

            pthread_mutex_lock(&connections_mutex);
            int ss_sock_local = connections[ss_slot].socket;
            pthread_mutex_unlock(&connections_mutex);

            pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
            MsgHeader rs;
            do {
                MsgHeader fwd = {0}; fwd.command = CMD_UNDO; fwd.payload_size = sizeof(req);
                if (!send_all(ss_sock_local, &fwd, sizeof(fwd)) || !send_all(ss_sock_local, &req, sizeof(req))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "Failed to contact Storage Server");
                    continue;
                }
                if (!recv_all(ss_sock_local, &rs, sizeof(rs))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "No response from Storage Server");
                    continue;
                }
                if (!send_all(socket, &rs, sizeof(rs))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    continue;
                }
                size_t rem = rs.payload_size; char buf[512];
                while (rem > 0) {
                    size_t chunk = rem > sizeof(buf) ? sizeof(buf) : rem;
                    if (!recv_all(ss_sock_local, buf, chunk)) break;
                    if (!send_all(socket, buf, chunk)) break;
                    rem -= chunk;
                }
            } while (0);
            pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
            continue;
        } else if ((header.command == CMD_TRASH_LIST || header.command == CMD_TRASH_RECOVER || header.command == CMD_TRASH_EMPTY) && connections[slot].type == CONN_TYPE_CLIENT) {
            // Handle Trash Bin operations
            if ((header.command == CMD_TRASH_LIST   && header.payload_size != (int)sizeof(MsgTrashList)) ||
                (header.command == CMD_TRASH_RECOVER&& header.payload_size != (int)sizeof(MsgTrashRecover)) ||
                (header.command == CMD_TRASH_EMPTY  && header.payload_size != (int)sizeof(MsgTrashEmpty))) {
                size_t rem = header.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, ch)) break; rem -= ch; }
                send_error(socket, 400, "Bad payload size");
                continue;
            }
            MsgTrashList tl; MsgTrashRecover tr; MsgTrashEmpty te;
            if (header.command == CMD_TRASH_LIST) { if (!recv_all(socket, &tl, sizeof(tl))) { send_error(socket, 400, "Payload read failed"); continue; } }
            else if (header.command == CMD_TRASH_RECOVER) { if (!recv_all(socket, &tr, sizeof(tr))) { send_error(socket, 400, "Payload read failed"); continue; } }
            else { if (!recv_all(socket, &te, sizeof(te))) { send_error(socket, 400, "Payload read failed"); continue; } }

            // Overwrite owner with authenticated username to enforce access
            pthread_mutex_lock(&connections_mutex);
            const char* auth_user = connections[slot].username;
            pthread_mutex_unlock(&connections_mutex);
            if (header.command == CMD_TRASH_LIST) { strncpy(tl.owner, auth_user, MAX_USERNAME_LEN-1); }
            else if (header.command == CMD_TRASH_RECOVER) { strncpy(tr.owner, auth_user, MAX_USERNAME_LEN-1); }
            else { strncpy(te.owner, auth_user, MAX_USERNAME_LEN-1); }

            if (header.command == CMD_TRASH_LIST) {
                // Query all SS and aggregate lists
                MsgTrashListResp agg = {0}; size_t pos = 0, cap = sizeof(agg.files);
                pthread_mutex_lock(&connections_mutex);
                for (int i = 0; i < MAX_CONNECTIONS; i++) {
                    if (connections[i].type != CONN_TYPE_SS || connections[i].socket < 0) continue;
                    int ss_sock = connections[i].socket;
                    pthread_mutex_unlock(&connections_mutex); // release while holding per-SS lock
                    pthread_mutex_lock(&connections[i].ss_io_mutex);
                    MsgHeader fwd = { .command = CMD_TRASH_LIST, .payload_size = sizeof(tl) };
                    if (!send_all(ss_sock, &fwd, sizeof(fwd)) || !send_all(ss_sock, &tl, sizeof(tl))) {
                        pthread_mutex_unlock(&connections[i].ss_io_mutex);
                        pthread_mutex_lock(&connections_mutex);
                        continue; // skip this SS
                    }
                    MsgHeader rs; if (!recv_all(ss_sock, &rs, sizeof(rs))) { pthread_mutex_unlock(&connections[i].ss_io_mutex); pthread_mutex_lock(&connections_mutex); continue; }
                    if (rs.command == CMD_TRASH_LIST_RESP && rs.payload_size == (int)sizeof(MsgTrashListResp)) {
                        MsgTrashListResp part = {0}; if (recv_all(ss_sock, &part, sizeof(part))) {
                            size_t len = strnlen(part.files, sizeof(part.files));
                            if (len > 0) {
                                if (pos + len >= cap) len = cap - pos - 1;
                                if (len > 0) { memcpy(agg.files + pos, part.files, len); pos += len; agg.files[pos] = '\0'; }
                            }
                        }
                    } else {
                        // Drain
                        size_t rem = rs.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(ss_sock, drain, ch)) break; rem -= ch; }
                    }
                    pthread_mutex_unlock(&connections[i].ss_io_mutex);
                    pthread_mutex_lock(&connections_mutex);
                }
                pthread_mutex_unlock(&connections_mutex);
                MsgHeader h = { .command = CMD_TRASH_LIST_RESP, .payload_size = sizeof(agg) };
                send_all(socket, &h, sizeof(h));
                send_all(socket, &agg, sizeof(agg));
                continue;
            }

            if (header.command == CMD_TRASH_RECOVER) {
                int recovered = 0; int recovered_ss = -1;
                pthread_mutex_lock(&connections_mutex);
                for (int i = 0; i < MAX_CONNECTIONS; i++) {
                    if (connections[i].type != CONN_TYPE_SS || connections[i].socket < 0) continue;
                    int ss_sock = connections[i].socket;
                    pthread_mutex_unlock(&connections_mutex);
                    pthread_mutex_lock(&connections[i].ss_io_mutex);
                    MsgHeader fwd = { .command = CMD_TRASH_RECOVER, .payload_size = sizeof(tr) };
                    if (!send_all(ss_sock, &fwd, sizeof(fwd)) || !send_all(ss_sock, &tr, sizeof(tr))) {
                        pthread_mutex_unlock(&connections[i].ss_io_mutex);
                        pthread_mutex_lock(&connections_mutex);
                        continue;
                    }
                    MsgHeader rs; if (!recv_all(ss_sock, &rs, sizeof(rs))) { pthread_mutex_unlock(&connections[i].ss_io_mutex); pthread_mutex_lock(&connections_mutex); continue; }
                    if (rs.command == CMD_ACK) {
                        recovered = 1; recovered_ss = i; pthread_mutex_unlock(&connections[i].ss_io_mutex); pthread_mutex_lock(&connections_mutex); break;
                    } else {
                        // Drain payload and try next
                        size_t rem = rs.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(ss_sock, drain, ch)) break; rem -= ch; }
                    }
                    pthread_mutex_unlock(&connections[i].ss_io_mutex);
                    pthread_mutex_lock(&connections_mutex);
                }
                pthread_mutex_unlock(&connections_mutex);
                if (!recovered) { send_error(socket, 404, "Not found in trash"); continue; }
                // Update registry: add mapping and refresh INFO
                const char* newname = (tr.newname[0] ? tr.newname : tr.filename);
                registry_add_file_if_absent(newname, tr.owner, recovered_ss);
                registry_update_one_from_ss(recovered_ss, newname);
                send_ack(socket);
                continue;
            }

            if (header.command == CMD_TRASH_EMPTY) {
                int ok_all = 1; int any_acted = 0;
                if (te.all) {
                    // Broadcast to all SS
                    pthread_mutex_lock(&connections_mutex);
                    for (int i = 0; i < MAX_CONNECTIONS; i++) {
                        if (connections[i].type != CONN_TYPE_SS || connections[i].socket < 0) continue;
                        int ss_sock = connections[i].socket;
                        pthread_mutex_unlock(&connections_mutex);
                        pthread_mutex_lock(&connections[i].ss_io_mutex);
                        MsgHeader fwd = { .command = CMD_TRASH_EMPTY, .payload_size = sizeof(te) };
                        if (!send_all(ss_sock, &fwd, sizeof(fwd)) || !send_all(ss_sock, &te, sizeof(te))) {
                            ok_all = 0; pthread_mutex_unlock(&connections[i].ss_io_mutex); pthread_mutex_lock(&connections_mutex); continue;
                        }
                        MsgHeader rs; if (!recv_all(ss_sock, &rs, sizeof(rs))) { ok_all = 0; pthread_mutex_unlock(&connections[i].ss_io_mutex); pthread_mutex_lock(&connections_mutex); continue; }
                        if (rs.command == CMD_ACK) { any_acted = 1; }
                        else { ok_all = 0; }
                        // Drain payload if any
                        size_t rem = rs.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(ss_sock, drain, ch)) break; rem -= ch; }
                        pthread_mutex_unlock(&connections[i].ss_io_mutex);
                        pthread_mutex_lock(&connections_mutex);
                    }
                    pthread_mutex_unlock(&connections_mutex);
                    if (!any_acted) { send_error(socket, 404, "Nothing to remove"); continue; }
                    if (!ok_all) { send_error(socket, 500, "Some items could not be removed"); continue; }
                    send_ack(socket); continue;
                } else {
                    // Remove a specific file from trash: try all SS until one ACKs
                    pthread_mutex_lock(&connections_mutex);
                    for (int i = 0; i < MAX_CONNECTIONS; i++) {
                        if (connections[i].type != CONN_TYPE_SS || connections[i].socket < 0) continue;
                        int ss_sock = connections[i].socket;
                        pthread_mutex_unlock(&connections_mutex);
                        pthread_mutex_lock(&connections[i].ss_io_mutex);
                        MsgHeader fwd = { .command = CMD_TRASH_EMPTY, .payload_size = sizeof(te) };
                        if (!send_all(ss_sock, &fwd, sizeof(fwd)) || !send_all(ss_sock, &te, sizeof(te))) {
                            pthread_mutex_unlock(&connections[i].ss_io_mutex); pthread_mutex_lock(&connections_mutex); continue;
                        }
                        MsgHeader rs; if (!recv_all(ss_sock, &rs, sizeof(rs))) { pthread_mutex_unlock(&connections[i].ss_io_mutex); pthread_mutex_lock(&connections_mutex); continue; }
                        if (rs.command == CMD_ACK) { any_acted = 1; pthread_mutex_unlock(&connections[i].ss_io_mutex); pthread_mutex_lock(&connections_mutex); break; }
                        // Drain and try next
                        size_t rem = rs.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(ss_sock, drain, ch)) break; rem -= ch; }
                        pthread_mutex_unlock(&connections[i].ss_io_mutex);
                        pthread_mutex_lock(&connections_mutex);
                    }
                    pthread_mutex_unlock(&connections_mutex);
                    if (!any_acted) { send_error(socket, 404, "Not found in trash"); continue; }
                    send_ack(socket); continue;
                }
            }
        } else if ((header.command == CMD_CHECKPOINT_CREATE || header.command == CMD_CHECKPOINT_VIEW || header.command == CMD_CHECKPOINT_REVERT || header.command == CMD_CHECKPOINT_LIST) && connections[slot].type == CONN_TYPE_CLIENT) {
            // Validate payload sizes by type and read into a union-like buffer
            if ((header.command == CMD_CHECKPOINT_CREATE && header.payload_size != (int)sizeof(MsgCheckpointCreate)) ||
                (header.command == CMD_CHECKPOINT_VIEW   && header.payload_size != (int)sizeof(MsgCheckpointView))   ||
                (header.command == CMD_CHECKPOINT_REVERT && header.payload_size != (int)sizeof(MsgCheckpointRevert)) ||
                (header.command == CMD_CHECKPOINT_LIST   && header.payload_size != (int)sizeof(MsgCheckpointList))) {
                // Drain
                size_t rem = header.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
                send_error(socket, 400, "Bad payload size");
                continue;
            }
            // Read payload
            MsgCheckpointCreate ccreate; MsgCheckpointView cview; MsgCheckpointRevert crevert; MsgCheckpointList clist;
            const char* fname = NULL;
            if (header.command == CMD_CHECKPOINT_CREATE) { if (!recv_all(socket, &ccreate, sizeof(ccreate))) { send_error(socket, 400, "Payload read failed"); continue; } fname = ccreate.filename; }
            else if (header.command == CMD_CHECKPOINT_VIEW) { if (!recv_all(socket, &cview, sizeof(cview))) { send_error(socket, 400, "Payload read failed"); continue; } fname = cview.filename; }
            else if (header.command == CMD_CHECKPOINT_REVERT) { if (!recv_all(socket, &crevert, sizeof(crevert))) { send_error(socket, 400, "Payload read failed"); continue; } fname = crevert.filename; }
            else { if (!recv_all(socket, &clist, sizeof(clist))) { send_error(socket, 400, "Payload read failed"); continue; } fname = clist.filename; }

            // Resolve owning SS (cache-first; refresh on miss)
            int ss_slot = -1;
            int idx_cp = file_index_find(fname);
            pthread_mutex_lock(&file_registry_mutex);
            if (idx_cp >= 0) ss_slot = file_registry[idx_cp].ss_slot;
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) {
                registry_refresh_from_ss();
                idx_cp = file_index_find(fname);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx_cp >= 0) ss_slot = file_registry[idx_cp].ss_slot;
                pthread_mutex_unlock(&file_registry_mutex);
            }
            if (ss_slot < 0) { send_error(socket, 404, "File not found"); continue; }

            // Overwrite requester with authenticated username
            pthread_mutex_lock(&connections_mutex);
            const char* auth_user = connections[slot].username;
            pthread_mutex_unlock(&connections_mutex);
            if (header.command == CMD_CHECKPOINT_CREATE) { strncpy(ccreate.requester, auth_user, MAX_USERNAME_LEN-1); }
            else if (header.command == CMD_CHECKPOINT_VIEW) { strncpy(cview.requester, auth_user, MAX_USERNAME_LEN-1); }
            else if (header.command == CMD_CHECKPOINT_REVERT) { strncpy(crevert.requester, auth_user, MAX_USERNAME_LEN-1); }
            else { strncpy(clist.requester, auth_user, MAX_USERNAME_LEN-1); }

            // Forward to SS with serialization; proxy response (including variable payload like VIEW)
            pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
            MsgHeader rs;
            do {
                int ss_sock_local;
                pthread_mutex_lock(&connections_mutex);
                ss_sock_local = connections[ss_slot].socket;
                pthread_mutex_unlock(&connections_mutex);

                MsgHeader fwd = { .command = header.command, .payload_size = header.payload_size };
                if (!send_all(ss_sock_local, &fwd, sizeof(fwd))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                // Send payload
                if (header.command == CMD_CHECKPOINT_CREATE) {
                    if (!send_all(ss_sock_local, &ccreate, sizeof(ccreate))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                } else if (header.command == CMD_CHECKPOINT_VIEW) {
                    if (!send_all(ss_sock_local, &cview, sizeof(cview))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                } else if (header.command == CMD_CHECKPOINT_REVERT) {
                    if (!send_all(ss_sock_local, &crevert, sizeof(crevert))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                } else {
                    if (!send_all(ss_sock_local, &clist, sizeof(clist))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                }

                if (!recv_all(ss_sock_local, &rs, sizeof(rs))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "No response from Storage Server");
                    continue;
                }
                if (!send_all(socket, &rs, sizeof(rs))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); continue; }
                size_t rem = rs.payload_size; char buf[4096];
                while (rem > 0) {
                    size_t chunk = rem > sizeof(buf) ? sizeof(buf) : rem;
                    if (!recv_all(ss_sock_local, buf, chunk)) break;
                    if (!send_all(socket, buf, chunk)) break;
                    rem -= chunk;
                }
                // If revert succeeded, refresh cached INFO for this file
                if (header.command == CMD_CHECKPOINT_REVERT && rs.command == CMD_ACK) {
                    registry_update_one_from_ss(ss_slot, fname);
                }
            } while (0);
            pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
            continue;
        } else if ((header.command == CMD_REQUEST_ACCESS || header.command == CMD_LIST_REQUESTS || header.command == CMD_RESPOND_REQUEST) && connections[slot].type == CONN_TYPE_CLIENT) {
            // Validate and read payload by type
            if ((header.command == CMD_REQUEST_ACCESS   && header.payload_size != (int)sizeof(MsgAccessRequestCreate)) ||
                (header.command == CMD_LIST_REQUESTS    && header.payload_size != (int)sizeof(MsgAccessRequestList))   ||
                (header.command == CMD_RESPOND_REQUEST  && header.payload_size != (int)sizeof(MsgAccessRequestRespond))) {
                size_t rem = header.payload_size; char drain[256];
                while (rem > 0) { size_t ch = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, ch)) break; rem -= ch; }
                send_error(socket, 400, "Bad payload size");
                continue;
            }
            // Read payloads
            MsgAccessRequestCreate creq; MsgAccessRequestList lreq; MsgAccessRequestRespond rreq;
            const char* fname = NULL;
            if (header.command == CMD_REQUEST_ACCESS) { if (!recv_all(socket, &creq, sizeof(creq))) { send_error(socket, 400, "Payload read failed"); continue; } fname = creq.filename; }
            else if (header.command == CMD_LIST_REQUESTS) { if (!recv_all(socket, &lreq, sizeof(lreq))) { send_error(socket, 400, "Payload read failed"); continue; } fname = lreq.filename; }
            else { if (!recv_all(socket, &rreq, sizeof(rreq))) { send_error(socket, 400, "Payload read failed"); continue; } fname = rreq.filename; }

            // Resolve owning SS (cache-first; refresh on miss)
            int ss_slot = -1;
            int idx_ar = file_index_find(fname);
            pthread_mutex_lock(&file_registry_mutex);
            if (idx_ar >= 0) ss_slot = file_registry[idx_ar].ss_slot;
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) {
                registry_refresh_from_ss();
                idx_ar = file_index_find(fname);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx_ar >= 0) ss_slot = file_registry[idx_ar].ss_slot;
                pthread_mutex_unlock(&file_registry_mutex);
            }
            if (ss_slot < 0) { send_error(socket, 404, "File not found"); continue; }

            // Overwrite requester with authenticated username
            const char* auth = connections[slot].username;
            if (header.command == CMD_REQUEST_ACCESS) { strncpy(creq.requester, auth, MAX_USERNAME_LEN-1); }
            else if (header.command == CMD_LIST_REQUESTS) { strncpy(lreq.requester, auth, MAX_USERNAME_LEN-1); }
            else { strncpy(rreq.requester, auth, MAX_USERNAME_LEN-1); }

            // Forward to SS with serialization and proxy response
            pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
            MsgHeader rs;
            do {
                int ss_sock_local;
                pthread_mutex_lock(&connections_mutex);
                ss_sock_local = connections[ss_slot].socket;
                pthread_mutex_unlock(&connections_mutex);

                MsgHeader fwd = { .command = header.command, .payload_size = header.payload_size };
                if (!send_all(ss_sock_local, &fwd, sizeof(fwd))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                if (header.command == CMD_REQUEST_ACCESS) {
                    if (!send_all(ss_sock_local, &creq, sizeof(creq))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                } else if (header.command == CMD_LIST_REQUESTS) {
                    if (!send_all(ss_sock_local, &lreq, sizeof(lreq))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                } else {
                    if (!send_all(ss_sock_local, &rreq, sizeof(rreq))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                }

                if (!recv_all(ss_sock_local, &rs, sizeof(rs))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "No response from Storage Server"); continue; }
                if (!send_all(socket, &rs, sizeof(rs))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); continue; }
                size_t rem = rs.payload_size; char buf[4096];
                while (rem > 0) { size_t ch = rem > sizeof(buf) ? sizeof(buf) : rem; if (!recv_all(ss_sock_local, buf, ch)) break; if (!send_all(socket, buf, ch)) break; rem -= ch; }
                // If approval succeeded, refresh cached INFO (ACL changed)
                if (header.command == CMD_RESPOND_REQUEST && rs.command == CMD_ACK && rreq.approve) {
                    registry_update_one_from_ss(ss_slot, fname);
                }
            } while (0);
            pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
            continue;
    } else if (header.command == CMD_LIST_USERS && connections[slot].type == CONN_TYPE_CLIENT) {
            // No payload expected; if present, drain it
            if (header.payload_size > 0) {
                size_t rem = header.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
            }

            // Assemble list of currently registered clients
            MsgUsersListResponse resp = {0};
            size_t pos = 0, cap = sizeof(resp.users);
            pthread_mutex_lock(&connections_mutex);
            for (int i = 0; i < MAX_CONNECTIONS; i++) {
                if (connections[i].type == CONN_TYPE_CLIENT && connections[i].username[0]) {
                    const char* u = connections[i].username;
                    size_t ulen = strnlen(u, MAX_USERNAME_LEN);
                    if (ulen + 1 <= cap - pos) {
                        memcpy(resp.users + pos, u, ulen); pos += ulen; resp.users[pos++] = '\n';
                    } else {
                        break; // buffer full
                    }
                }
            }
            pthread_mutex_unlock(&connections_mutex);

            MsgHeader h = { .command = CMD_LIST_USERS_RESP, .payload_size = sizeof(resp) };
            send_all(socket, &h, sizeof(h));
            send_all(socket, &resp, sizeof(resp));
            continue;
        } else if (header.command == CMD_EXEC && connections[slot].type == CONN_TYPE_CLIENT) {
            if (header.payload_size != sizeof(MsgExecRequest)) {
                send_error(socket, 400, "Bad payload size");
                // Drain
                size_t rem = header.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
                continue;
            }
            MsgExecRequest rq;
            if (!recv_all(socket, &rq, sizeof(rq))) { send_error(socket, 400, "Payload read failed"); continue; }
            // Overwrite requester with authenticated username
            strncpy(rq.requester, connections[slot].username, MAX_USERNAME_LEN-1);
            rq.requester[MAX_USERNAME_LEN-1] = '\0';

            // Resolve owning SS (cache-first; refresh on miss)
            int ss_slot = -1;
            int idx_exec = file_index_find(rq.filename);
            pthread_mutex_lock(&file_registry_mutex);
            if (idx_exec >= 0) ss_slot = file_registry[idx_exec].ss_slot;
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) {
                registry_refresh_from_ss();
                idx_exec = file_index_find(rq.filename);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx_exec >= 0) ss_slot = file_registry[idx_exec].ss_slot;
                pthread_mutex_unlock(&file_registry_mutex);
            }
            if (ss_slot < 0) { send_error(socket, 404, "File not found"); continue; }

            // Contact SS's client port to fetch file content using header-based READ (SS enforces ACL)
            char ss_ip[MAX_IP_LEN] = {0};
            int ss_port = 0;
            pthread_mutex_lock(&connections_mutex);
            strncpy(ss_ip, connections[ss_slot].ip_addr, sizeof(ss_ip)-1);
            ss_port = connections[ss_slot].client_port;
            pthread_mutex_unlock(&connections_mutex);

            char* file_data = NULL; int file_size = 0;
            int ff_rc = nm_fetch_file_from_ss(ss_ip, ss_port, rq.filename, rq.requester, &file_data, &file_size);
            if (ff_rc != 0) {
                if (ff_rc <= -1000) { // encoded SS error
                    int code = -1000 - ff_rc; send_error(socket, code ? code : 502, "READ denied or failed");
                } else {
                    send_error(socket, 502, "Failed to fetch file from SS");
                }
                if (file_data) free(file_data);
                continue;
            }

            // Execute on Name Server host using /bin/sh -s < tmpfile, capturing stdout+stderr
            char tmpl[] = "/tmp/nm_exec_XXXXXX";
            int tfd = mkstemp(tmpl);
            if (tfd < 0) {
                free(file_data);
                send_error(socket, 500, "mkstemp failed");
                continue;
            }
            ssize_t wr = 0; ssize_t to_write = file_size; char* p = file_data;
            while (to_write > 0) { ssize_t n = write(tfd, p, (size_t)to_write > 8192 ? 8192 : (size_t)to_write); if (n <= 0) break; to_write -= n; p += n; wr += n; }
            close(tfd);

            char cmd[512]; snprintf(cmd, sizeof(cmd), "/bin/sh -s < %s 2>&1", tmpl);
            FILE* pp = popen(cmd, "r");
            if (!pp) {
                unlink(tmpl);
                free(file_data);
                send_error(socket, 500, "popen failed");
                continue;
            }
            // Keep the temp file until the shell finishes; unlink after pclose to avoid races
            free(file_data);

            // Read all output
            char* out = NULL; size_t out_sz = 0; size_t out_cap = 0;
            char obuf[4096]; size_t r;
            while ((r = fread(obuf, 1, sizeof(obuf), pp)) > 0) {
                if (out_sz + r + 1 > out_cap) { size_t new_cap = out_cap ? out_cap * 2 : 8192; while (new_cap < out_sz + r + 1) new_cap *= 2; char* nbuf = realloc(out, new_cap); if (!nbuf) { out_sz = 0; break; } out = nbuf; out_cap = new_cap; }
                memcpy(out + out_sz, obuf, r); out_sz += r; out[out_sz] = '\0';
            }
            pclose(pp);
            unlink(tmpl);

            // Send output as ACK payload (if no output, send zero-length ACK)
            if (!out) {
                MsgHeader ah0 = { .command = CMD_ACK, .payload_size = 0 };
                send_all(socket, &ah0, sizeof(ah0));
                continue;
            }
            MsgHeader ah = { .command = CMD_ACK, .payload_size = (int)out_sz };
            if (!send_all(socket, &ah, sizeof(ah)) || (out_sz > 0 && !send_all(socket, out, out_sz))) {
                // client disconnected; drop
            }
            free(out);
            continue;
        }

    if (header.command == CMD_DELETE_FILE && connections[slot].type == CONN_TYPE_CLIENT) {
            if (header.payload_size != sizeof(MsgDeleteFile)) {
                send_error(socket, 400, "Bad payload size");
                continue;
            }
            MsgDeleteFile payload;
            if (!recv_all(socket, &payload, sizeof(payload))) {
                send_error(socket, 400, "Failed to read payload");
                continue;
            }

            // Overwrite requester with authenticated username
            strncpy(payload.requester, connections[slot].username, MAX_USERNAME_LEN-1);
            payload.requester[MAX_USERNAME_LEN-1] = '\0';

            // Resolve owning SS for this file (cache-first; refresh on miss)
            int ss_slot = -1;
            int idx_del = file_index_find(payload.filename);
            pthread_mutex_lock(&file_registry_mutex);
            if (idx_del >= 0) ss_slot = file_registry[idx_del].ss_slot;
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) {
                registry_refresh_from_ss();
                idx_del = file_index_find(payload.filename);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx_del >= 0) ss_slot = file_registry[idx_del].ss_slot;
                pthread_mutex_unlock(&file_registry_mutex);
            }
            if (ss_slot < 0) {
                send_error(socket, 404, "File not found");
                continue;
            }

            // Forward to SS with per-SS serialization
            pthread_mutex_lock(&connections_mutex);
            int ss_sock_local = connections[ss_slot].socket;
            pthread_mutex_unlock(&connections_mutex);
         LOG_NM("Forwarding DELETE '%s' to SS at %s:%d (Slot %d)\n",
             payload.filename, connections[ss_slot].ip_addr, connections[ss_slot].client_port, ss_slot);

            MsgHeader ss_resp;
            pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
            do {
                MsgHeader fwd = {0};
                fwd.command = CMD_DELETE_FILE;
                fwd.payload_size = sizeof(MsgDeleteFile);
                if (!send_all(ss_sock_local, &fwd, sizeof(fwd)) ||
                    !send_all(ss_sock_local, &payload, sizeof(payload))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "Failed to contact Storage Server");
                    continue;
                }

                // Receive SS response and proxy it back to the client
                if (!recv_all(ss_sock_local, &ss_resp, sizeof(ss_resp))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "No response from Storage Server");
                    continue;
                }
                // Send header to client now
                if (!send_all(socket, &ss_resp, sizeof(ss_resp))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    continue;
                }

                // Forward payload in full (if any) while holding lock
                size_t remaining = ss_resp.payload_size;
                char buf[4096];
                while (remaining > 0) {
                    size_t chunk = remaining > sizeof(buf) ? sizeof(buf) : remaining;
                    if (!recv_all(ss_sock_local, buf, chunk)) {
                        pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                        send_error(socket, 502, "Failed to read SS payload");
                        break;
                    }
                    if (!send_all(socket, buf, chunk)) {
                        break;
                    }
                    remaining -= chunk;
                }
            } while (0);
            pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
            // Optionally, remove from registry on success
            if (ss_resp.command == CMD_ACK) {
                pthread_mutex_lock(&file_registry_mutex);
                for (int i = 0; i < file_count; i++) {
                    if (strncmp(file_registry[i].filename, payload.filename, MAX_FILENAME_LEN) == 0) {
                        // compact array
                        for (int j = i + 1; j < file_count; j++) file_registry[j-1] = file_registry[j];
                        file_count--;
                        break;
                    }
                }
                pthread_mutex_unlock(&file_registry_mutex);
                // Rebuild fast index after mutation
                file_index_build();
            }
            continue;
        }

        if (header.command == CMD_CLEAR_FILE && connections[slot].type == CONN_TYPE_CLIENT) {
            if (header.payload_size != sizeof(MsgClearFile)) {
                send_error(socket, 400, "Bad payload size");
                continue;
            }
            MsgClearFile payload;
            if (!recv_all(socket, &payload, sizeof(payload))) {
                send_error(socket, 400, "Failed to read payload");
                continue;
            }

            // Overwrite requester with authenticated username
            strncpy(payload.requester, connections[slot].username, MAX_USERNAME_LEN-1);
            payload.requester[MAX_USERNAME_LEN-1] = '\0';

            // Resolve owning SS for this file (cache-first; refresh on miss)
            int ss_slot = -1;
            int idx_clr = file_index_find(payload.filename);
            pthread_mutex_lock(&file_registry_mutex);
            if (idx_clr >= 0) ss_slot = file_registry[idx_clr].ss_slot;
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) {
                registry_refresh_from_ss();
                idx_clr = file_index_find(payload.filename);
                pthread_mutex_lock(&file_registry_mutex);
                if (idx_clr >= 0) ss_slot = file_registry[idx_clr].ss_slot;
                pthread_mutex_unlock(&file_registry_mutex);
            }
            if (ss_slot < 0) {
                send_error(socket, 404, "File not found");
                continue;
            }

            // Forward to SS with per-SS serialization
            pthread_mutex_lock(&connections_mutex);
            int ss_sock_local = connections[ss_slot].socket;
            pthread_mutex_unlock(&connections_mutex);
            LOG_NM("Forwarding CLEAR '%s' to SS at %s:%d (Slot %d)\n",
                   payload.filename, connections[ss_slot].ip_addr, connections[ss_slot].client_port, ss_slot);

            MsgHeader ss_resp;
            pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
            do {
                MsgHeader fwd = {0};
                fwd.command = CMD_CLEAR_FILE;
                fwd.payload_size = sizeof(MsgClearFile);
                if (!send_all(ss_sock_local, &fwd, sizeof(fwd)) ||
                    !send_all(ss_sock_local, &payload, sizeof(payload))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "Failed to contact Storage Server");
                    continue;
                }

                if (!recv_all(ss_sock_local, &ss_resp, sizeof(ss_resp))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    send_error(socket, 502, "No response from Storage Server");
                    continue;
                }
                // Relay header
                if (!send_all(socket, &ss_resp, sizeof(ss_resp))) {
                    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    continue;
                }

                // Drain/relay payload if any
                size_t remaining = ss_resp.payload_size; char buf[4096];
                while (remaining > 0) {
                    size_t chunk = remaining > sizeof(buf) ? sizeof(buf) : remaining;
                    if (!recv_all(ss_sock_local, buf, chunk)) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to read SS payload"); break; }
                    if (!send_all(socket, buf, chunk)) { break; }
                    remaining -= chunk;
                }
            } while (0);
            pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
            continue;
        }

        // TODO: handle other client commands...
    }
    
    // If recv_all fails, the client disconnected
    LOG_NM("Connection from %s (Socket %d, Slot %d) disconnected.\n", ip, socket, slot);
    
    // --- CLEANUP ---
    // Free the slot
    pthread_mutex_lock(&connections_mutex);
    if (connections[slot].type == CONN_TYPE_SS) {
    LOG_NM("Removed Storage Server (Slot %d) from active list.\n", slot);
        // Destroy per-SS mutex
        pthread_mutex_destroy(&connections[slot].ss_io_mutex);
    } else if (connections[slot].type == CONN_TYPE_CLIENT) {
    LOG_NM("Removed Client '%s' (Slot %d) from active list.\n", connections[slot].username, slot);
    }
    connections[slot].type = CONN_TYPE_FREE;
    connections[slot].socket = -1;
    pthread_mutex_unlock(&connections_mutex);

    close(socket);
    return NULL;
}

/**
 * @brief Handles the CMD_REGISTER_SS workflow.
 */
void handle_register_ss(int slot, const MsgRegisterSS* msg) {
    pthread_mutex_lock(&connections_mutex);
    connections[slot].type = CONN_TYPE_SS;
    connections[slot].client_port = msg->client_listen_port;
    pthread_mutex_init(&connections[slot].ss_io_mutex, NULL);
    pthread_mutex_unlock(&connections_mutex);

    LOG_NM("Storage Server registered from %s. Listening for clients on port %d. (Slot: %d)\n",
        connections[slot].ip_addr, msg->client_listen_port, slot);

    send_ack(connections[slot].socket);

    // Populate/refresh registry mapping when a new SS registers (infrequent, acceptable cost)
    registry_refresh_from_ss();
}

/**
 * @brief Handles the CMD_REGISTER_CLIENT workflow.
 */
void handle_register_client(int slot, const MsgRegisterClient* msg) {
    pthread_mutex_lock(&connections_mutex);
    connections[slot].type = CONN_TYPE_CLIENT;
    // Ensure username is null-terminated
    strncpy(connections[slot].username, msg->username, MAX_USERNAME_LEN - 1);
    connections[slot].username[MAX_USERNAME_LEN - 1] = '\0';
    pthread_mutex_unlock(&connections_mutex);

    LOG_NM("Client '%s' registered from %s. (Slot: %d)\n",
        connections[slot].username, connections[slot].ip_addr, slot);
           
    send_ack(connections[slot].socket);
}

/**
 * @brief Sends a simple CMD_ACK response.
 */
void send_ack(int socket) {
    MsgHeader ack_header;
    ack_header.command = CMD_ACK;
    ack_header.payload_size = 0;
    
    if (!send_all(socket, &ack_header, sizeof(MsgHeader))) {
        LOGE_NM("Failed to send ACK to socket %d\n", socket);
    }
}

// Helper to pick an SS (first-fit for now)
static int choose_ss_slot() {
    int ss_slot = -1;
    pthread_mutex_lock(&connections_mutex);
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (connections[i].type == CONN_TYPE_SS && connections[i].socket >= 0) {
            ss_slot = i;
            break;
        }
    }
    pthread_mutex_unlock(&connections_mutex);
    return ss_slot;
}

static void send_error(int socket, int code, const char* msg) {
    MsgHeader h = {0};
    MsgError  e = {0};
    h.command = CMD_ERROR;
    h.payload_size = sizeof(MsgError);
    e.code = code;
    strncpy(e.message, msg ? msg : "error", sizeof(e.message)-1);
    send_all(socket, &h, sizeof(h));
    send_all(socket, &e, sizeof(e));
}

static void registry_add_file_if_absent(const char* filename, const char* owner, int ss_slot) {
    if (!filename || !*filename) return;
    int do_insert_index = 0;
    int new_idx = -1;
    pthread_mutex_lock(&file_registry_mutex);
    for (int i = 0; i < file_count; i++) {
        if (strncmp(file_registry[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            if (file_registry[i].ss_slot < 0 && ss_slot >= 0) {
                file_registry[i].ss_slot = ss_slot;
            }
            pthread_mutex_unlock(&file_registry_mutex);
            return; // already present
        }
    }
    if (file_count < MAX_FILES) {
        new_idx = file_count;
        strncpy(file_registry[new_idx].filename, filename, MAX_FILENAME_LEN - 1);
        if (owner) strncpy(file_registry[new_idx].owner, owner, MAX_USERNAME_LEN - 1);
        file_registry[new_idx].ss_slot = ss_slot;
        file_count++;
        do_insert_index = 1;
    }
    pthread_mutex_unlock(&file_registry_mutex);
    // Update fast index outside of file_registry_mutex to avoid lock order inversion
    if (do_insert_index) {
        file_index_insert(filename, new_idx);
    }
}

// Query all connected SS for their file lists and rebuild the registry (union)
static void registry_refresh_from_ss(void) {
    // Build a new mapping from filenames to ss_slot
    int names_cap = 2048;
    char (*names)[MAX_FILENAME_LEN] = calloc((size_t)names_cap, MAX_FILENAME_LEN);
    int* slots = calloc((size_t)names_cap, sizeof(int));
    int ncount = 0;

    pthread_mutex_lock(&connections_mutex);
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (connections[i].type != CONN_TYPE_SS || connections[i].socket < 0) continue;
        int ss_sock = connections[i].socket;

        // Serialize on this SS socket while requesting file list
        pthread_mutex_lock(&connections[i].ss_io_mutex);

        // Send request
        MsgHeader rq = {0};
        rq.command = CMD_SS_LIST_FILES;
        rq.payload_size = 0;
        if (!send_all(ss_sock, &rq, sizeof(rq))) {
            pthread_mutex_unlock(&connections[i].ss_io_mutex);
            continue; // skip this SS
        }

        // Read response header
        MsgHeader rs;
        if (!recv_all(ss_sock, &rs, sizeof(rs))) {
            pthread_mutex_unlock(&connections[i].ss_io_mutex);
            continue;
        }

        MsgSSFileListResponse pl = {0};
        if (rs.command == CMD_SS_LIST_FILES_RESP && rs.payload_size == sizeof(MsgSSFileListResponse)) {
            if (!recv_all(ss_sock, &pl, sizeof(pl))) {
                pthread_mutex_unlock(&connections[i].ss_io_mutex);
                continue;
            }
        } else {
            // Drain unexpected payload if any
            size_t rem = rs.payload_size; char drain[512];
            while (rem > 0) {
                size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem;
                if (!recv_all(ss_sock, drain, chunk)) break;
                rem -= chunk;
            }
            pthread_mutex_unlock(&connections[i].ss_io_mutex);
            continue;
        }

        pthread_mutex_unlock(&connections[i].ss_io_mutex);

        // Parse newline-separated filenames, dedupe, and store mapping to this SS slot
        char* p = pl.files;
        while (p && *p) {
            char* nl = strchr(p, '\n');
            size_t l = nl ? (size_t)(nl - p) : strlen(p);
            if (l > 0 && l < MAX_FILENAME_LEN) {
                char tmp[MAX_FILENAME_LEN];
                memcpy(tmp, p, l); tmp[l] = '\0';
                int exists = 0;
                for (int k = 0; k < ncount; k++) {
                    if (strncmp(names[k], tmp, MAX_FILENAME_LEN) == 0) { exists = 1; break; }
                }
                if (!exists && ncount < names_cap) {
                    strncpy(names[ncount], tmp, MAX_FILENAME_LEN - 1);
                    names[ncount][MAX_FILENAME_LEN - 1] = '\0';
                    slots[ncount] = i;
                    ncount++;
                }
            }
            if (!nl) break; else p = nl + 1;
        }
    }
    pthread_mutex_unlock(&connections_mutex);

    // Replace registry with the new mapping and populate cached INFO for each file
    pthread_mutex_lock(&file_registry_mutex);
    file_count = 0;
    for (int i = 0; i < ncount && i < MAX_FILES; i++) {
        strncpy(file_registry[file_count].filename, names[i], MAX_FILENAME_LEN - 1);
        file_registry[file_count].filename[MAX_FILENAME_LEN - 1] = '\0';
        file_registry[file_count].owner[0] = '\0';
        file_registry[file_count].ss_slot = slots[i];
        file_registry[file_count].created = 0;
        file_registry[file_count].updated = 0;
        file_registry[file_count].size_bytes = 0;
        file_registry[file_count].words_cnt = 0;
        file_registry[file_count].chars_cnt = 0;
        file_registry[file_count].last_access = 0;
        file_registry[file_count].last_modified = 0;
        file_registry[file_count].readers[0] = '\0';
        file_registry[file_count].writers[0] = '\0';
        file_registry[file_count].info_str[0] = '\0';
        file_count++;
    }
    pthread_mutex_unlock(&file_registry_mutex);

    // Rebuild the fast index from the refreshed registry
    file_index_build();

    // Populate cached INFO for each file from respective SS
    for (int i = 0; i < ncount; i++) {
        registry_update_one_from_ss(slots[i], names[i]);
    }

    free(names);
    free(slots);
}

// Helper: refresh cached INFO for a single file owned by ss_slot
static void registry_update_one_from_ss(int ss_slot, const char* filename) {
    if (ss_slot < 0 || !filename || !*filename) return;

    // Validate SS connection
    pthread_mutex_lock(&connections_mutex);
    int valid = (ss_slot < MAX_CONNECTIONS && connections[ss_slot].type == CONN_TYPE_SS && connections[ss_slot].socket >= 0);
    int ss_sock = valid ? connections[ss_slot].socket : -1;
    pthread_mutex_unlock(&connections_mutex);
    if (!valid) return;

    MsgInfoResponse info = {0};
    int got = 0;
    pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
    do {
        MsgHeader ih = { .command = CMD_INFO, .payload_size = sizeof(MsgInfoRequest) };
        MsgInfoRequest ir = {0};
        strncpy(ir.filename, filename, MAX_FILENAME_LEN-1);
        if (!send_all(ss_sock, &ih, sizeof(ih)) || !send_all(ss_sock, &ir, sizeof(ir))) break;
        MsgHeader rh;
        if (!recv_all(ss_sock, &rh, sizeof(rh))) break;
        if (rh.command != CMD_INFO_RESP || rh.payload_size != sizeof(MsgInfoResponse)) {
            // drain
            size_t rem = rh.payload_size; char drain[512];
            while (rem > 0) { size_t ch = rem>sizeof(drain)?sizeof(drain):rem; if (!recv_all(ss_sock, drain, ch)) break; rem -= ch; }
            break;
        }
        if (!recv_all(ss_sock, &info, sizeof(info))) break;
        got = 1;
    } while (0);
    pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);

    if (!got) return;

    // Parse INFO into registry cache
    pthread_mutex_lock(&file_registry_mutex);
    int idx = -1;
    for (int i = 0; i < file_count; i++) {
        if (strncmp(file_registry[i].filename, filename, MAX_FILENAME_LEN) == 0) { idx = i; break; }
    }
    if (idx >= 0) {
        // Preserve the full info string
        strncpy(file_registry[idx].info_str, info.info, sizeof(file_registry[idx].info_str)-1);
        file_registry[idx].info_str[sizeof(file_registry[idx].info_str)-1] = '\0';

        // Extract fields
        const char* p = info.info;
        char owner[MAX_USERNAME_LEN] = {0};
        long long created=0, updated=0, size=0, words=0, chars=0, last_access=0, last_mod=0;
        char readers[1024] = {0}, writers[1024] = {0};
        while (*p) {
            const char* nl = strchr(p, '\n'); size_t len = nl ? (size_t)(nl - p) : strlen(p);
            if (len >= 6 && strncmp(p, "owner:", 6) == 0) { sscanf(p+6, "%63s", owner); }
            else if (len >= 8 && strncmp(p, "created:", 8) == 0) { const char* par = strchr(p, '('); if (par) created = atoll(par+1); }
            else if (len >= 8 && strncmp(p, "updated:", 8) == 0) { const char* par = strchr(p, '('); if (par) updated = atoll(par+1); }
            else if (len >= 5 && strncmp(p, "size:", 5) == 0) { sscanf(p+5, "%lld", &size); }
            else if (len >= 6 && strncmp(p, "words:", 6) == 0) { sscanf(p+6, "%lld", &words); }
            else if (len >= 6 && strncmp(p, "chars:", 6) == 0) { sscanf(p+6, "%lld", &chars); }
            else if (len >= 12 && strncmp(p, "last_access:", 12) == 0) { const char* par = strchr(p, '('); if (par) last_access = atoll(par+1); }
            else if (len >= 14 && strncmp(p, "last_modified:", 14) == 0) { const char* par = strchr(p, '('); if (par) last_mod = atoll(par+1); }
            else if (len >= 8 && strncmp(p, "readers:", 8) == 0) { size_t cp = len-8; if (cp > sizeof(readers)-1) cp = sizeof(readers)-1; memcpy(readers, p+8, cp); readers[cp] = '\0'; }
            else if (len >= 8 && strncmp(p, "writers:", 8) == 0) { size_t cp = len-8; if (cp > sizeof(writers)-1) cp = sizeof(writers)-1; memcpy(writers, p+8, cp); writers[cp] = '\0'; }
            if (!nl) break; p = nl + 1;
        }
        strncpy(file_registry[idx].owner, owner, sizeof(file_registry[idx].owner)-1);
        file_registry[idx].created = created;
        file_registry[idx].updated = updated;
        file_registry[idx].size_bytes = size;
        file_registry[idx].words_cnt = words;
        file_registry[idx].chars_cnt = chars;
        file_registry[idx].last_access = last_access;
        file_registry[idx].last_modified = last_mod;
        // normalize ACL strings (trim)
        // Remove trailing newlines/spaces
        size_t rlen = strlen(readers); while (rlen>0 && (readers[rlen-1]=='\r'||readers[rlen-1]=='\n'||readers[rlen-1]==' '||readers[rlen-1]=='\t')) readers[--rlen]=0;
        size_t wlen = strlen(writers); while (wlen>0 && (writers[wlen-1]=='\r'||writers[wlen-1]=='\n'||writers[wlen-1]==' '||writers[wlen-1]=='\t')) writers[--wlen]=0;
        strncpy(file_registry[idx].readers, readers, sizeof(file_registry[idx].readers)-1);
        strncpy(file_registry[idx].writers, writers, sizeof(file_registry[idx].writers)-1);
    }
    pthread_mutex_unlock(&file_registry_mutex);
}
