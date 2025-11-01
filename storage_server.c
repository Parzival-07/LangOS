#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <dirent.h>

int client_listen_port;
int nm_socket;

#ifndef STORAGE_ROOT
#define STORAGE_ROOT "ss_data"
#endif

static int ensure_dir(const char* path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        return S_ISDIR(st.st_mode) ? 0 : -1;
    }
    #ifdef _WIN32
    int rc = _mkdir(path);
    #else
    int rc = mkdir(path, 0755);
    #endif
    return (rc == 0) ? 0 : -1;
}

static int is_valid_filename(const char* s) {
    if (!s || !*s) return 0;
    for (const char* p = s; *p; ++p) {
        char c = *p;
        if (!( (c>='a'&&c<='z') || (c>='A'&&c<='Z') || (c>='0'&&c<='9') || c=='_' || c=='-' || c=='.')) {
            return 0;
        }
    }
    return 1;
}

static int file_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int ss_create_file_do(const MsgCreateFile* m, char* errbuf, size_t errlen) {
    if (ensure_dir(STORAGE_ROOT) != 0) {
        snprintf(errbuf, errlen, "Failed to create storage dir");
        return -1;
    }
    if (!is_valid_filename(m->filename)) {
        snprintf(errbuf, errlen, "Invalid filename");
        return -1;
    }
    char txt[512], meta[512];

    // Store content exactly as provided (no extra .txt)
    snprintf(txt, sizeof(txt),  STORAGE_ROOT "/%s",       m->filename);
    snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", m->filename);

    if (file_exists(txt) || file_exists(meta)) {
        snprintf(errbuf, errlen, "File already exists");
        return -1;
    }

    FILE* f = fopen(txt, "w");
    if (!f) {
        snprintf(errbuf, errlen, "Cannot create content file");
        return -1;
    }
    fclose(f);

    FILE* mf = fopen(meta, "w");
    if (!mf) {
        snprintf(errbuf, errlen, "Cannot create metadata file");
        return -1;
    }
    time_t now = time(NULL);
    fprintf(mf, "owner:%s\n", m->owner);
    fprintf(mf, "created:%lld\n", (long long)now);
    fprintf(mf, "updated:%lld\n", (long long)now);
    fprintf(mf, "readers: \n");
    fprintf(mf, "writers: \n");
    fclose(mf);

    return 0;
}

// Delete the content file and its metadata. Returns 0 on success, -1 on failure.
static int ss_delete_file_do(const char* filename) {
    if (!is_valid_filename(filename)) {
        return -1;
    }
    if (ensure_dir(STORAGE_ROOT) != 0) {
        return -1;
    }
    char txt[512], meta[512];
    snprintf(txt, sizeof(txt),  STORAGE_ROOT "/%s",       filename);
    snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", filename);

    // Attempt to remove both files; both must succeed per spec
    if (remove(txt) != 0) {
        return -1;
    }
    if (remove(meta) != 0) {
        return -1;
    }
    return 0;
}

/**
 * @brief Thread to listen for direct Client connections (for READ, WRITE, etc.)
 */
void* handle_client_connections(void* arg) {
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    // Create listening socket for Clients
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("[SS] Client socket creation failed");
        return NULL;
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("[SS] setsockopt");
        return NULL;
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(client_listen_port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("[SS] Client socket bind failed");
        return NULL;
    }
    if (listen(server_fd, 5) < 0) {
        perror("[SS] Client socket listen failed");
        return NULL;
    }

    printf("[SS] Storage Server now listening for CLIENTS on port %d...\n",
           client_listen_port);

    // --- Accept Loop for Clients ---
    while (true) {
        int client_socket;
        if ((client_socket = accept(server_fd, NULL, NULL)) < 0) {
            perror("[SS] Client accept failed");
            continue;
        }
        printf("[SS] Received a direct client connection!\n");
        
        // Simple protocol: client sends a single line with the filename, we return file content and close.
        char fname[MAX_FILENAME_LEN+2];
        size_t fpos = 0;
        while (fpos < sizeof(fname)-1) {
            char ch;
            ssize_t r = recv(client_socket, &ch, 1, 0);
            if (r <= 0) break; // disconnect or error
            if (ch == '\n') break;
            fname[fpos++] = ch;
        }
        fname[fpos] = '\0';

        if (fpos == 0 || !is_valid_filename(fname)) {
            const char* msg = "ERROR: invalid filename\n";
            send_all(client_socket, msg, strlen(msg));
            close(client_socket);
            continue;
        }

        char path[512];
        snprintf(path, sizeof(path), STORAGE_ROOT "/%s", fname);
        FILE* f = fopen(path, "rb");
        if (!f) {
            const char* msg = "ERROR: not found\n";
            send_all(client_socket, msg, strlen(msg));
            close(client_socket);
            continue;
        }
        char buf[4096];
        size_t nread;
        while ((nread = fread(buf, 1, sizeof(buf), f)) > 0) {
            if (!send_all(client_socket, buf, nread)) {
                break;
            }
        }
        fclose(f);
        close(client_socket);
    }
    return NULL;
}

static void nm_send_error(int code, const char* msg) {
    MsgHeader h = {0};
    MsgError  e = {0};
    h.command = CMD_ERROR;
    h.payload_size = sizeof(MsgError);
    e.code = code;
    strncpy(e.message, msg ? msg : "error", sizeof(e.message)-1);
    send_all(nm_socket, &h, sizeof(h));
    send_all(nm_socket, &e, sizeof(e));
}

/**
 * @brief Thread to listen for commands from the Name Server
 */
void* listen_to_nm(void* arg) {
    MsgHeader header;
    
    // Loop forever, waiting for commands from the NM
    while (true) {
        if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
            fprintf(stderr, "[SS] Connection to Name Server lost! Exiting.\n");
            exit(EXIT_FAILURE); // If NM is down, SS can't function
        }

        printf("[SS] Received command %d from Name Server.\n", header.command);
        
        // Handle commands from NM
        switch (header.command) {
            case CMD_CREATE_FILE: {
                if (header.payload_size != sizeof(MsgCreateFile)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgCreateFile msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) {
                    nm_send_error(400, "Payload read failed");
                    break;
                }
                char err[256] = {0};
                int rc = ss_create_file_do(&msg, err, sizeof(err));
                if (rc == 0) {
                    printf("[SS] Created file '%s' for owner '%s'\n", msg.filename, msg.owner);
                    MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                    send_all(nm_socket, &ack, sizeof(ack));
                } else {
                    printf("[SS] CREATE failed for '%s': %s\n", msg.filename, err);
                    nm_send_error(409, err[0] ? err : "CREATE failed");
                }
                break;
            }
            case CMD_SS_LIST_FILES: {
                // Drain any unexpected payload
                if (header.payload_size > 0) {
                    char drain[512];
                    size_t remaining = header.payload_size;
                    while (remaining > 0) {
                        size_t chunk = remaining > sizeof(drain) ? sizeof(drain) : remaining;
                        if (!recv_all(nm_socket, drain, chunk)) break;
                        remaining -= chunk;
                    }
                }

                MsgSSFileListResponse resp = {0};
                size_t pos = 0, cap = sizeof(resp.files);

                DIR* d = opendir(STORAGE_ROOT);
                if (d) {
                    struct dirent* ent;
                    while ((ent = readdir(d)) != NULL) {
                        const char* name = ent->d_name;
                        // Skip . and ..
                        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;
                        // Skip metadata files
                        size_t nlen = strlen(name);
                        if (nlen > 5 && strcmp(name + nlen - 5, ".meta") == 0) continue;

                        int n = snprintf(resp.files + pos, (pos < cap ? cap - pos : 0), "%s\n", name);
                        if (n <= 0) break;
                        if ((size_t)n >= (cap - pos)) { // truncated; stop
                            pos = cap - 1;
                            break;
                        }
                        pos += (size_t)n;
                    }
                    closedir(d);
                }

                MsgHeader h = {0};
                h.command = CMD_SS_LIST_FILES_RESP;
                h.payload_size = sizeof(resp);
                send_all(nm_socket, &h, sizeof(h));
                send_all(nm_socket, &resp, sizeof(resp));
                break;
            }
            case CMD_DELETE_FILE: {
                if (header.payload_size != sizeof(MsgDeleteFile)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgDeleteFile msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) {
                    nm_send_error(400, "Payload read failed");
                    break;
                }
                int rc = ss_delete_file_do(msg.filename);
                if (rc == 0) {
                    printf("[SS] Deleted file '%s'\n", msg.filename);
                    MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                    send_all(nm_socket, &ack, sizeof(ack));
                } else {
                    printf("[SS] DELETE failed for '%s'\n", msg.filename);
                    nm_send_error(404, "DELETE failed");
                }
                break;
            }
            default:
                // TODO: other commands in Phase 2
                break;
        }
    }
    return NULL;
}

int main(int argc, char const *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <client_listen_port>\n", argv[0]);
        return 1;
    }

    client_listen_port = atoi(argv[1]);
    if (client_listen_port <= 0 || client_listen_port > 65535) {
        fprintf(stderr, "Invalid port number.\n");
        return 1;
    }

    // --- Start Thread to Listen for Clients ---
    pthread_t client_thread_id;
    if (pthread_create(&client_thread_id, NULL, handle_client_connections, NULL) != 0) {
        perror("[SS] Failed to create client listener thread");
        return 1;
    }

    // --- Connect to Name Server ---
    struct sockaddr_in nm_address;
    if ((nm_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("[SS] Socket creation error");
        return 1;
    }

    nm_address.sin_family = AF_INET;
    nm_address.sin_port = htons(NAME_SERVER_PORT);

    // Convert IPv4 address from text to binary form
    if (inet_pton(AF_INET, "127.0.0.1", &nm_address.sin_addr) <= 0) {
        perror("[SS] Invalid address/ Address not supported");
        return 1;
    }

    printf("[SS] Connecting to Name Server at 127.0.0.1:%d...\n", NAME_SERVER_PORT);

    if (connect(nm_socket, (struct sockaddr *)&nm_address, sizeof(nm_address)) < 0) {
        perror("[SS] Connection Failed");
        return 1;
    }

    printf("[SS] Connected to Name Server!\n");

    // Ensure storage root exists
    if (ensure_dir(STORAGE_ROOT) != 0) {
        fprintf(stderr, "[SS] Failed to ensure storage root '%s'\n", STORAGE_ROOT);
        return 1;
    }

    // --- Register with Name Server ---
    MsgHeader header;
    MsgRegisterSS reg_msg;

    header.command = CMD_REGISTER_SS;
    header.payload_size = sizeof(MsgRegisterSS);
    reg_msg.client_listen_port = client_listen_port;

    printf("[SS] Registering with NM (Client Port: %d)...\n", client_listen_port);

    // Send header, then payload
    if (!send_all(nm_socket, &header, sizeof(MsgHeader))) return 1;
    if (!send_all(nm_socket, &reg_msg, sizeof(MsgRegisterSS))) return 1;


    // --- Wait for ACK ---
    if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
        fprintf(stderr, "[SS] Failed to receive ACK from NM.\n");
        return 1;
    }

    if (header.command == CMD_ACK) {
        printf("[SS] Registration successful!\n");
    } else {
        fprintf(stderr, "[SS] Registration failed (Received command %d).\n", header.command);
        return 1;
    }
    
    // --- Start Thread to Listen for NM Commands ---
    pthread_t nm_thread_id;
    if(pthread_create(&nm_thread_id, NULL, listen_to_nm, NULL) != 0) {
        perror("[SS] Failed to create NM listener thread");
        return 1;
    }

    // Wait for threads to finish (which they won't)
    pthread_join(client_thread_id, NULL);
    pthread_join(nm_thread_id, NULL);
    
    close(nm_socket);
    return 0;
}
