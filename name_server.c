#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

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
} FileRecord;

static FileRecord file_registry[MAX_FILES];
static int file_count = 0;
static pthread_mutex_t file_registry_mutex = PTHREAD_MUTEX_INITIALIZER;

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

// --- MAIN SERVER LOGIC ---

int main() {
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

    printf("[NM] Name Server listening on port %d...\n", NAME_SERVER_PORT);

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

        printf("[NM] New connection accepted from %s\n", args->ip_addr);

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
        fprintf(stderr, "[NM] Max connections reached. Rejecting %s.\n", ip);
        // TODO: Send CMD_ERROR (Server Busy)
        close(socket);
        return NULL;
    }

    MsgHeader header;
    // Read the first header to see who this is
    if (!recv_all(socket, &header, sizeof(MsgHeader))) {
        fprintf(stderr, "[NM] Failed to read header from %s. Disconnecting.\n", ip);
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
                fprintf(stderr, "[NM] Failed to read REG_SS payload from %s.\n", ip);
                registration_successful = false;
            } else {
                handle_register_ss(slot, &msg);
            }
            break;
        }
        case CMD_REGISTER_CLIENT: {
            MsgRegisterClient msg;
            if (!recv_all(socket, &msg, sizeof(MsgRegisterClient))) {
                fprintf(stderr, "[NM] Failed to read REG_CLIENT payload from %s.\n", ip);
                registration_successful = false;
            } else {
                handle_register_client(slot, &msg);
            }
            break;
        }
        default:
            fprintf(stderr, "[NM] Unknown command %d from %s.\n", header.command, ip);
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
    printf("[NM] Connection from %s (Socket %d, Slot %d) registered. Now in idle loop.\n", ip, socket, slot);

    // IMPORTANT: Do not read from SS sockets here, or you'll steal replies
    // that client threads are synchronously waiting to proxy.
    if (connections[slot].type == CONN_TYPE_SS) {
        // Park this thread; keep the socket open for client threads to use.
        while (true) {
            sleep(3600);
        }
    }

    while (recv_all(socket, &header, sizeof(MsgHeader))) {
        printf("[NM] Received command %d from socket %d (Slot %d)\n", header.command, socket, slot);

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
                printf("[NM] No Storage Server available for CREATE '%s'\n", payload.filename);
                send_error(socket, 503, "No Storage Server available");
                continue;
            }

            // Forward to SS with per-SS serialization
            int ss_sock;
            pthread_mutex_lock(&connections_mutex);
            ss_sock = connections[ss_slot].socket;
            pthread_mutex_unlock(&connections_mutex);
            printf("[NM] Forwarding CREATE '%s' (owner=%s) to SS at %s:%d (Slot %d)\n",
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
            continue;
        } else if (header.command == CMD_VIEW_FILES && connections[slot].type == CONN_TYPE_CLIENT) {
            // Before serving VIEW, refresh registry from connected SS to prune stale entries
            registry_refresh_from_ss();

            // Expect a MsgViewFilesRequest payload
            if (header.payload_size != sizeof(MsgViewFilesRequest)) {
                // Drain unexpected payload
                size_t rem = header.payload_size; char drain[512];
                while (rem > 0) { size_t chunk = rem > sizeof(drain)?sizeof(drain):rem; if (!recv_all(socket, drain, chunk)) break; rem -= chunk; }
                send_error(socket, 400, "Bad payload size");
                continue;
            }
            MsgViewFilesRequest vreq = {0};
            if (!recv_all(socket, &vreq, sizeof(vreq))) { send_error(socket, 400, "Payload read failed"); continue; }
            // Use authenticated username regardless of payload
            char requester[MAX_USERNAME_LEN] = {0};
            strncpy(requester, connections[slot].username, MAX_USERNAME_LEN-1);

            // Build result
            char buffer[16384]; buffer[0] = '\0'; size_t pos = 0, cap = sizeof(buffer);

            pthread_mutex_lock(&file_registry_mutex);
            for (int i = 0; i < file_count; i++) {
                const char* fname = file_registry[i].filename;
                int ss_slot = file_registry[i].ss_slot;
                int include = 1; // default include for -a
                char linebuf[2048] = {0};

                if (!vreq.show_all) {
                    // Query SS for INFO and check ACL locally
                    include = 0;
                }

                // Always fetch INFO if long_list or need to check access
                MsgInfoResponse info = {0};
                int have_info = 0;
                if (vreq.long_list || !vreq.show_all) {
                    pthread_mutex_lock(&connections_mutex);
                    int valid = (ss_slot >= 0 && connections[ss_slot].type == CONN_TYPE_SS && connections[ss_slot].socket >= 0);
                    int ss_sock_local = valid ? connections[ss_slot].socket : -1;
                    pthread_mutex_unlock(&connections_mutex);
                    if (valid) {
                        pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
                        MsgHeader ih = { .command = CMD_INFO, .payload_size = sizeof(MsgInfoRequest) };
                        MsgInfoRequest ir = {0}; strncpy(ir.filename, fname, MAX_FILENAME_LEN-1);
                        if (send_all(ss_sock_local, &ih, sizeof(ih)) && send_all(ss_sock_local, &ir, sizeof(ir))) {
                            MsgHeader rh; if (recv_all(ss_sock_local, &rh, sizeof(rh))) {
                                if (rh.command == CMD_INFO_RESP && rh.payload_size == sizeof(MsgInfoResponse)) {
                                    if (recv_all(ss_sock_local, &info, sizeof(info))) { have_info = 1; }
                                } else {
                                    // Drain unexpected
                                    size_t rem = rh.payload_size; char drain[512]; while (rem > 0) { size_t ch = rem>sizeof(drain)?sizeof(drain):rem; if (!recv_all(ss_sock_local, drain, ch)) break; rem -= ch; }
                                }
                            }
                        }
                        pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
                    }
                }

                if (!vreq.show_all) {
                    if (have_info) {
                        // Parse owner/readers/writers from info.info
                        char owner[MAX_USERNAME_LEN] = {0};
                        char readers[1024] = {0};
                        char writers[1024] = {0};
                        // Simple parse
                        const char* p = info.info;
                        while (*p) {
                            const char* nl = strchr(p, '\n'); size_t len = nl ? (size_t)(nl - p) : strlen(p);
                            if (len >= 6 && strncmp(p, "owner:", 6) == 0) { sscanf(p+6, "%63s", owner); }
                            else if (len >= 8 && strncmp(p, "readers:", 8) == 0) { strncpy(readers, p+8, sizeof(readers)-1); }
                            else if (len >= 8 && strncmp(p, "writers:", 8) == 0) { strncpy(writers, p+8, sizeof(writers)-1); }
                            if (!nl) break; p = nl + 1;
                        }
                        // Trim whitespace from readers/writers simple way
                        // Check access: owner or listed in readers/writers
                        int allowed = 0;
                        if (owner[0] && strncmp(owner, requester, MAX_USERNAME_LEN)==0) allowed = 1;
                        // tokenize by space/comma
                        if (!allowed) {
                            char tmp[1024]; strncpy(tmp, readers, sizeof(tmp)-1); tmp[sizeof(tmp)-1]=0; char* ctx=NULL; char* t=strtok_r(tmp, ", \t\r\n", &ctx); while(t){ if(strcmp(t, requester)==0){allowed=1; break;} t=strtok_r(NULL, ", \t\r\n", &ctx);} }
                        if (!allowed) {
                            char tmp2[1024]; strncpy(tmp2, writers, sizeof(tmp2)-1); tmp2[sizeof(tmp2)-1]=0; char* ctx2=NULL; char* t2=strtok_r(tmp2, ", \t\r\n", &ctx2); while(t2){ if(strcmp(t2, requester)==0){allowed=1; break;} t2=strtok_r(NULL, ", \t\r\n", &ctx2);} }
                        include = allowed;
                    } else {
                        include = 0; // no info -> cannot validate access
                    }
                }

                if (!include) continue;

                if (vreq.long_list && have_info) {
                    // Include condensed one-line details: owner, size, words, chars, last_access
                    // Extract fields from info.info
                    char owner[128]="", size[64]="", words[64]="", chars[64]="", last[128]="", lastmod[128]="";
                    const char* p2 = info.info;
                    while (*p2) {
                        const char* nl = strchr(p2, '\n'); size_t len = nl ? (size_t)(nl - p2) : strlen(p2);
                        if (len >= 6 && strncmp(p2, "owner:", 6) == 0) { sscanf(p2+6, "%127s", owner); }
                        else if (len >= 5 && strncmp(p2, "size:", 5) == 0) { sscanf(p2+5, "%63s", size); }
                        else if (len >= 6 && strncmp(p2, "words:", 6) == 0) { sscanf(p2+6, "%63s", words); }
                        else if (len >= 6 && strncmp(p2, "chars:", 6) == 0) { sscanf(p2+6, "%63s", chars); }
                        else if (len >= 12 && strncmp(p2, "last_access:", 12) == 0) {
                            // Copy whole line after label (timestamp may have spaces)
                            size_t cplen = len - 12; if (cplen > sizeof(last)-1) cplen = sizeof(last)-1; memcpy(last, p2+12, cplen); last[cplen] = 0;
                        } else if (len >= 14 && strncmp(p2, "last_modified:", 14) == 0) {
                            size_t cplen = len - 14; if (cplen > sizeof(lastmod)-1) cplen = sizeof(lastmod)-1; memcpy(lastmod, p2+14, cplen); lastmod[cplen] = 0;
                        }
                        if (!nl) break; p2 = nl + 1;
                    }
                    snprintf(linebuf, sizeof(linebuf), "%s\towner:%s\tsize:%s\twords:%s\tchars:%s\tlast_access:%s\tlast_modified:%s\n", fname, owner, size, words, chars, last[0]?last:"-", lastmod[0]?lastmod:"-");
                } else {
                    snprintf(linebuf, sizeof(linebuf), "%s\n", fname);
                }

                size_t ln = strlen(linebuf);
                if (pos + ln >= cap) { /* stop if would overflow */ break; }
                memcpy(buffer + pos, linebuf, ln); pos += ln; buffer[pos] = '\0';
            }
            pthread_mutex_unlock(&file_registry_mutex);

            MsgViewFilesResponse resp = {0};
            strncpy(resp.file_list, buffer, sizeof(resp.file_list) - 1);
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

            // Ensure mapping is up-to-date
            registry_refresh_from_ss();

            MsgReadFileResponse resp = {0};
            resp.found = 0;

            pthread_mutex_lock(&file_registry_mutex);
            int idx = -1;
            for (int i = 0; i < file_count; i++) {
                if (strncmp(file_registry[i].filename, req.filename, MAX_FILENAME_LEN) == 0) { idx = i; break; }
            }
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

            // Ensure mapping up-to-date and find owning SS
            registry_refresh_from_ss();
            int ss_slot = -1;
            pthread_mutex_lock(&file_registry_mutex);
            for (int i = 0; i < file_count; i++) {
                if (strncmp(file_registry[i].filename, req.filename, MAX_FILENAME_LEN) == 0) {
                    ss_slot = file_registry[i].ss_slot;
                    break;
                }
            }
            pthread_mutex_unlock(&file_registry_mutex);
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

            // Find owning SS
            registry_refresh_from_ss();
            int ss_slot = -1;
            pthread_mutex_lock(&file_registry_mutex);
            for (int i = 0; i < file_count; i++) {
                if (strncmp(file_registry[i].filename, req.filename, MAX_FILENAME_LEN) == 0) { ss_slot = file_registry[i].ss_slot; break; }
            }
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) { send_error(socket, 404, "File not found"); continue; }

            pthread_mutex_lock(&connections_mutex);
            int ss_sock_local = connections[ss_slot].socket;
            pthread_mutex_unlock(&connections_mutex);

            pthread_mutex_lock(&connections[ss_slot].ss_io_mutex);
            MsgHeader rs;
            do {
                MsgHeader fwd = {0}; fwd.command = CMD_INFO; fwd.payload_size = sizeof(req);
                if (!send_all(ss_sock_local, &fwd, sizeof(fwd)) || !send_all(ss_sock_local, &req, sizeof(req))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "Failed to contact Storage Server"); continue; }
                if (!recv_all(ss_sock_local, &rs, sizeof(rs))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); send_error(socket, 502, "No response from Storage Server"); continue; }
                if (!send_all(socket, &rs, sizeof(rs))) { pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex); continue; }
                size_t rem = rs.payload_size; char buf[512];
                while (rem > 0) { size_t chunk = rem > sizeof(buf) ? sizeof(buf) : rem; if (!recv_all(ss_sock_local, buf, chunk)) break; if (!send_all(socket, buf, chunk)) break; rem -= chunk; }
            } while (0);
            pthread_mutex_unlock(&connections[ss_slot].ss_io_mutex);
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

            // Find owning SS
            registry_refresh_from_ss();
            int ss_slot = -1;
            pthread_mutex_lock(&file_registry_mutex);
            for (int i = 0; i < file_count; i++) {
                if (strncmp(file_registry[i].filename, req.filename, MAX_FILENAME_LEN) == 0) { ss_slot = file_registry[i].ss_slot; break; }
            }
            pthread_mutex_unlock(&file_registry_mutex);
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

            // Resolve owning SS for this file
            registry_refresh_from_ss();
            int ss_slot = -1;
            pthread_mutex_lock(&file_registry_mutex);
            for (int i = 0; i < file_count; i++) {
                if (strncmp(file_registry[i].filename, payload.filename, MAX_FILENAME_LEN) == 0) { ss_slot = file_registry[i].ss_slot; break; }
            }
            pthread_mutex_unlock(&file_registry_mutex);
            if (ss_slot < 0) {
                send_error(socket, 404, "File not found");
                continue;
            }

            // Forward to SS with per-SS serialization
            pthread_mutex_lock(&connections_mutex);
            int ss_sock_local = connections[ss_slot].socket;
            pthread_mutex_unlock(&connections_mutex);
            printf("[NM] Forwarding DELETE '%s' to SS at %s:%d (Slot %d)\n",
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
            }
            continue;
        }

        // TODO: handle other client commands...
    }
    
    // If recv_all fails, the client disconnected
    printf("[NM] Connection from %s (Socket %d, Slot %d) disconnected.\n", ip, socket, slot);
    
    // --- CLEANUP ---
    // Free the slot
    pthread_mutex_lock(&connections_mutex);
    if (connections[slot].type == CONN_TYPE_SS) {
        printf("[NM] Removed Storage Server (Slot %d) from active list.\n", slot);
        // Destroy per-SS mutex
        pthread_mutex_destroy(&connections[slot].ss_io_mutex);
    } else if (connections[slot].type == CONN_TYPE_CLIENT) {
        printf("[NM] Removed Client '%s' (Slot %d) from active list.\n", connections[slot].username, slot);
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

    printf("[NM] Storage Server registered from %s. Listening for clients on port %d. (Slot: %d)\n",
           connections[slot].ip_addr, msg->client_listen_port, slot);

    send_ack(connections[slot].socket);
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

    printf("[NM] Client '%s' registered from %s. (Slot: %d)\n",
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
        fprintf(stderr, "[NM] Failed to send ACK to socket %d\n", socket);
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
        strncpy(file_registry[file_count].filename, filename, MAX_FILENAME_LEN - 1);
        if (owner) strncpy(file_registry[file_count].owner, owner, MAX_USERNAME_LEN - 1);
        file_registry[file_count].ss_slot = ss_slot;
        file_count++;
    }
    pthread_mutex_unlock(&file_registry_mutex);
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

    // Replace registry with the new mapping
    pthread_mutex_lock(&file_registry_mutex);
    file_count = 0;
    for (int i = 0; i < ncount && i < MAX_FILES; i++) {
        strncpy(file_registry[file_count].filename, names[i], MAX_FILENAME_LEN - 1);
        file_registry[file_count].filename[MAX_FILENAME_LEN - 1] = '\0';
        file_registry[file_count].owner[0] = '\0';
        file_registry[file_count].ss_slot = slots[i];
        file_count++;
    }
    pthread_mutex_unlock(&file_registry_mutex);

    free(names);
    free(slots);
}
