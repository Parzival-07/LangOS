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
static void registry_add_file_if_absent(const char* filename, const char* owner);
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

            // Forward to SS
            int ss_sock = connections[ss_slot].socket;
            printf("[NM] Forwarding CREATE '%s' (owner=%s) to SS at %s:%d (Slot %d)\n",
                   payload.filename, payload.owner, connections[ss_slot].ip_addr, connections[ss_slot].client_port, ss_slot);

            MsgHeader fwd = {0};
            fwd.command = CMD_CREATE_FILE;
            fwd.payload_size = sizeof(MsgCreateFile);
            if (!send_all(ss_sock, &fwd, sizeof(fwd)) ||
                !send_all(ss_sock, &payload, sizeof(payload))) {
                send_error(socket, 502, "Failed to contact Storage Server");
                continue;
            }

            // Receive SS response here and proxy it back to the client
            MsgHeader ss_resp;
            if (!recv_all(ss_sock, &ss_resp, sizeof(ss_resp))) {
                send_error(socket, 502, "No response from Storage Server");
                continue;
            }

            // If SS acknowledged creation, update NM registry before proxying
            if (ss_resp.command == CMD_ACK) {
                registry_add_file_if_absent(payload.filename, payload.owner);
            }

            // Send header to client
            if (!send_all(socket, &ss_resp, sizeof(ss_resp))) {
                // Client disconnected
                continue;
            }

            // Forward payload in full (if any)
            size_t remaining = ss_resp.payload_size;
            char buf[4096];
            while (remaining > 0) {
                size_t chunk = remaining > sizeof(buf) ? sizeof(buf) : remaining;
                if (!recv_all(ss_sock, buf, chunk)) {
                    send_error(socket, 502, "Failed to read SS payload");
                    break;
                }
                if (!send_all(socket, buf, chunk)) {
                    break;
                }
                remaining -= chunk;
            }
            continue;
        } else if (header.command == CMD_VIEW_FILES && connections[slot].type == CONN_TYPE_CLIENT) {
            // Before serving VIEW, refresh registry from connected SS to prune stale entries
            registry_refresh_from_ss();
            // No payload expected for simple VIEW
            if (header.payload_size != 0) {
                // Drain any unexpected payload
                char drain[1024];
                size_t remaining = header.payload_size;
                while (remaining > 0) {
                    size_t chunk = remaining > sizeof(drain) ? sizeof(drain) : remaining;
                    if (!recv_all(socket, drain, chunk)) break;
                    remaining -= chunk;
                }
            }

            // Build newline-separated list
            char buffer[16384];
            buffer[0] = '\0';
            size_t pos = 0, cap = sizeof(buffer);

            pthread_mutex_lock(&file_registry_mutex);
            for (int i = 0; i < file_count; i++) {
                int n = snprintf(buffer + pos, (pos < cap ? cap - pos : 0), "%s\n", file_registry[i].filename);
                if (n <= 0) break;
                if ((size_t)n >= (cap - pos)) { // truncated; stop
                    pos = cap - 1;
                    break;
                }
                pos += (size_t)n;
            }
            pthread_mutex_unlock(&file_registry_mutex);

            MsgViewFilesResponse resp = {0};
            strncpy(resp.file_list, buffer, sizeof(resp.file_list) - 1);

            MsgHeader h = {0};
            h.command = CMD_VIEW_FILES_RESP;
            h.payload_size = sizeof(MsgViewFilesResponse);

            if (!send_all(socket, &h, sizeof(h)) ||
                !send_all(socket, &resp, sizeof(resp))) {
                // Client disconnected
            }
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

            int ss_slot = choose_ss_slot();
            if (ss_slot < 0) {
                printf("[NM] No Storage Server available for DELETE '%s'\n", payload.filename);
                send_error(socket, 503, "No Storage Server available");
                continue;
            }

            // Forward to SS
            int ss_sock = connections[ss_slot].socket;
            printf("[NM] Forwarding DELETE '%s' to SS at %s:%d (Slot %d)\n",
                   payload.filename, connections[ss_slot].ip_addr, connections[ss_slot].client_port, ss_slot);

            MsgHeader fwd = {0};
            fwd.command = CMD_DELETE_FILE;
            fwd.payload_size = sizeof(MsgDeleteFile);
            if (!send_all(ss_sock, &fwd, sizeof(fwd)) ||
                !send_all(ss_sock, &payload, sizeof(payload))) {
                send_error(socket, 502, "Failed to contact Storage Server");
                continue;
            }

            // Receive SS response and proxy it back to the client
            MsgHeader ss_resp;
            if (!recv_all(ss_sock, &ss_resp, sizeof(ss_resp))) {
                send_error(socket, 502, "No response from Storage Server");
                continue;
            }

            // Send header to client
            if (!send_all(socket, &ss_resp, sizeof(ss_resp))) {
                // Client disconnected
                continue;
            }

            // Forward payload in full (if any)
            size_t remaining = ss_resp.payload_size;
            char buf[4096];
            while (remaining > 0) {
                size_t chunk = remaining > sizeof(buf) ? sizeof(buf) : remaining;
                if (!recv_all(ss_sock, buf, chunk)) {
                    send_error(socket, 502, "Failed to read SS payload");
                    break;
                }
                if (!send_all(socket, buf, chunk)) {
                    break;
                }
                remaining -= chunk;
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

static void registry_add_file_if_absent(const char* filename, const char* owner) {
    if (!filename || !*filename) return;
    pthread_mutex_lock(&file_registry_mutex);
    for (int i = 0; i < file_count; i++) {
        if (strncmp(file_registry[i].filename, filename, MAX_FILENAME_LEN) == 0) {
            pthread_mutex_unlock(&file_registry_mutex);
            return; // already present
        }
    }
    if (file_count < MAX_FILES) {
        strncpy(file_registry[file_count].filename, filename, MAX_FILENAME_LEN - 1);
        if (owner) strncpy(file_registry[file_count].owner, owner, MAX_USERNAME_LEN - 1);
        file_count++;
    }
    pthread_mutex_unlock(&file_registry_mutex);
}

// Query all connected SS for their file lists and rebuild the registry (union)
static void registry_refresh_from_ss(void) {
    // Collect union into a dynamic buffer to avoid large stack usage
    size_t ucap = 32768;
    char* union_list = (char*)calloc(ucap, 1);
    if (!union_list) {
        return;
    }
    size_t upos = 0;

    pthread_mutex_lock(&connections_mutex);
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        if (connections[i].type != CONN_TYPE_SS || connections[i].socket < 0) continue;
        int ss_sock = connections[i].socket;

        // Send request
        MsgHeader rq = {0};
        rq.command = CMD_SS_LIST_FILES;
        rq.payload_size = 0;
        if (!send_all(ss_sock, &rq, sizeof(rq))) {
            continue; // skip this SS
        }

        // Read response header
        MsgHeader rs;
        if (!recv_all(ss_sock, &rs, sizeof(rs))) {
            continue;
        }
        if (rs.command != CMD_SS_LIST_FILES_RESP || rs.payload_size != sizeof(MsgSSFileListResponse)) {
            // Drain payload if any
            size_t rem = rs.payload_size;
            char drain[512];
            while (rem > 0) {
                size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem;
                if (!recv_all(ss_sock, drain, chunk)) break;
                rem -= chunk;
            }
            continue;
        }

        // Read payload
        MsgSSFileListResponse pl = {0};
        if (!recv_all(ss_sock, &pl, sizeof(pl))) {
            continue;
        }

        // Append to union_list (will dedupe later)
        size_t len = strnlen(pl.files, sizeof(pl.files));
        if (len == 0) continue;
        if (upos + len >= ucap) len = ucap - upos - 1;
        if (len > 0) {
            memcpy(union_list + upos, pl.files, len);
            upos += len;
            union_list[upos] = '\0';
        }
    }
    pthread_mutex_unlock(&connections_mutex);

    // Rebuild registry from union_list (dedupe by filename)
    // We'll scan union_list line by line and add unique names
    char* cursor = union_list;
    // Build a temp array to dedupe (heap-allocated to avoid stack overflow)
    int names_cap = 2048;
    char (*names)[MAX_FILENAME_LEN] = calloc((size_t)names_cap, MAX_FILENAME_LEN);
    int ncount = 0;

    while (cursor && *cursor) {
        char* nl = strchr(cursor, '\n');
        size_t l = nl ? (size_t)(nl - cursor) : strlen(cursor);
        if (l > 0 && l < MAX_FILENAME_LEN) {
            char tmp[MAX_FILENAME_LEN];
            memcpy(tmp, cursor, l);
            tmp[l] = '\0';
            // dedupe
            int found = 0;
            for (int k = 0; k < ncount; k++) {
                if (strncmp(names[k], tmp, MAX_FILENAME_LEN) == 0) { found = 1; break; }
            }
            if (!found && ncount < names_cap) {
                strncpy(names[ncount], tmp, MAX_FILENAME_LEN - 1);
                names[ncount][MAX_FILENAME_LEN - 1] = '\0';
                ncount++;
            }
        }
        if (!nl) break;
        cursor = nl + 1;
    }

    // Merge into registry: we’ll replace existing registry with the union
    pthread_mutex_lock(&file_registry_mutex);
    file_count = 0;
    for (int i = 0; i < ncount && i < MAX_FILES; i++) {
        strncpy(file_registry[file_count].filename, names[i], MAX_FILENAME_LEN - 1);
        file_registry[file_count].filename[MAX_FILENAME_LEN - 1] = '\0';
        file_registry[file_count].owner[0] = '\0'; // owner unknown after refresh
        file_count++;
    }
    pthread_mutex_unlock(&file_registry_mutex);

    free(names);
    free(union_list);
}
