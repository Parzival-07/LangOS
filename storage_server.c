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
#include <ctype.h>

// --- Helpers for parsing and updating access lists ---
static int list_contains(const char* list, const char* user) {
    if (!user || !*user) return 0;
    char tmp[1024];
    strncpy(tmp, list ? list : "", sizeof(tmp)-1);
    tmp[sizeof(tmp)-1] = 0;
    for (char* p = tmp; *p; ++p) { if (*p==','||*p=='\n'||*p=='\r'||*p=='\t') *p=' '; }
    char* ctx = NULL; char* tok = strtok_r(tmp, " ", &ctx);
    while (tok) { if (strcmp(tok, user) == 0) return 1; tok = strtok_r(NULL, " ", &ctx); }
    return 0;
}

static void list_append(char* list, size_t cap, const char* user) {
    if (!user || !*user) return;
    size_t len = strlen(list);
    if (len == 0) {
        strncat(list, user, cap - strlen(list) - 1);
    } else {
        strncat(list, ",", cap - strlen(list) - 1);
        strncat(list, user, cap - strlen(list) - 1);
    }
}

static int list_remove(char* list, const char* user) {
    char tmp[1024]; strncpy(tmp, list ? list : "", sizeof(tmp)-1); tmp[sizeof(tmp)-1] = 0;
    char out[1024] = "";
    char* ctx = NULL; char* tok = strtok_r(tmp, ", \t\r\n", &ctx);
    int first = 1;
    int removed = 0;
    while (tok) {
        if (strcmp(tok, user) != 0) {
            if (!first) strncat(out, ",", sizeof(out)-1);
            strncat(out, tok, sizeof(out)-1);
            first = 0;
        } else {
            removed++;
        }
        tok = strtok_r(NULL, ", \t\r\n", &ctx);
    }
    strncpy(list, out, 1023);
    list[1023] = 0;
    return removed;
}

static void trim_both(char* s) {
    if (!s) return;
    // left trim
    char* p = s;
    while (*p && isspace((unsigned char)*p)) p++;
    if (p != s) {
        size_t len = strlen(p);
        memmove(s, p, len + 1);
    }
    // right trim
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n-1])) { s[n-1] = 0; n--; }
}

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
    // Write empty lists with no trailing space after colon for stable formatting
    fprintf(mf, "readers:\n");
    fprintf(mf, "writers:\n");
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
            case CMD_ADD_ACCESS:
            case CMD_REM_ACCESS: {
                if (header.payload_size != sizeof(MsgAccessChange)) {
                    nm_send_error(400, "Bad payload size");
                    break;
                }
                MsgAccessChange msg;
                if (!recv_all(nm_socket, &msg, sizeof(msg))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(msg.filename)) { nm_send_error(400, "Invalid filename"); break; }

                // Load metadata
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", msg.filename);
                FILE* mf = fopen(meta, "r");
                if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0};
                long long created=0, updated=0;
                char readers[1024] = {0}, writers[1024] = {0};
                char line[2048];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                    else if (strncmp(line, "created:", 8) == 0) { sscanf(line+8, "%lld", &created); }
                    else if (strncmp(line, "updated:", 8) == 0) { sscanf(line+8, "%lld", &updated); }
                    else if (strncmp(line, "readers:", 8) == 0) { strncpy(readers, line+8, sizeof(readers)-1); }
                    else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                }
                fclose(mf);
                // Trim leading and trailing whitespace so formatting is stable
                trim_both(readers);
                trim_both(writers);

                // Owner check
                if (owner[0]==0 || strncmp(owner, msg.requester, MAX_USERNAME_LEN) != 0) {
                    nm_send_error(403, "Only owner may change access");
                    break;
                }

                if (header.command == CMD_ADD_ACCESS) {
                    if (msg.is_writer) {
                        if (list_contains(writers, msg.target)) { nm_send_error(409, "User already has writer access"); break; }
                        list_append(writers, sizeof(writers), msg.target);
                    } else {
                        if (list_contains(readers, msg.target)) { nm_send_error(409, "User already has reader access"); break; }
                        list_append(readers, sizeof(readers), msg.target);
                    }
                } else { // REM_ACCESS
                    if (msg.is_writer) {
                        if (!list_contains(writers, msg.target)) { nm_send_error(404, "User not found in writers"); break; }
                        (void)list_remove(writers, msg.target);
                    } else {
                        if (!list_contains(readers, msg.target)) { nm_send_error(404, "User not found in readers"); break; }
                        (void)list_remove(readers, msg.target);
                    }
                }

                // Save metadata back (only on success)
                time_t now = time(NULL); updated = (long long)now;
                mf = fopen(meta, "w");
                if (!mf) { nm_send_error(500, "Failed to write meta"); break; }
                fprintf(mf, "owner:%s\n", owner);
                fprintf(mf, "created:%lld\n", created);
                fprintf(mf, "updated:%lld\n", updated);
                fprintf(mf, "readers:%s%s\n", (readers[0]?" ":""), readers);
                fprintf(mf, "writers:%s%s\n", (writers[0]?" ":""), writers);
                fclose(mf);

                MsgHeader ack = { .command = CMD_ACK, .payload_size = 0 };
                send_all(nm_socket, &ack, sizeof(ack));
                break;
            }
            case CMD_INFO: {
                if (header.payload_size != sizeof(MsgInfoRequest)) { nm_send_error(400, "Bad payload size"); break; }
                MsgInfoRequest req; if (!recv_all(nm_socket, &req, sizeof(req))) { nm_send_error(400, "Payload read failed"); break; }
                if (!is_valid_filename(req.filename)) { nm_send_error(400, "Invalid filename"); break; }
                char meta[512]; snprintf(meta, sizeof(meta), STORAGE_ROOT "/%s.meta", req.filename);
                FILE* mf = fopen(meta, "r"); if (!mf) { nm_send_error(404, "File not found"); break; }
                char owner[MAX_USERNAME_LEN] = {0};
                long long created=0, updated=0;
                char readers[1024] = {0}, writers[1024] = {0};
                char line[2048];
                while (fgets(line, sizeof(line), mf)) {
                    if (strncmp(line, "owner:", 6) == 0) { sscanf(line+6, "%63s", owner); }
                    else if (strncmp(line, "created:", 8) == 0) { sscanf(line+8, "%lld", &created); }
                    else if (strncmp(line, "updated:", 8) == 0) { sscanf(line+8, "%lld", &updated); }
                    else if (strncmp(line, "readers:", 8) == 0) { strncpy(readers, line+8, sizeof(readers)-1); }
                    else if (strncmp(line, "writers:", 8) == 0) { strncpy(writers, line+8, sizeof(writers)-1); }
                }
                fclose(mf);
                // Trim leading and trailing whitespace
                trim_both(readers);
                trim_both(writers);
                // Build info string
                MsgInfoResponse resp = {0};
                char created_buf[64]={0}, updated_buf[64]={0};
                time_t cr=(time_t)created, up=(time_t)updated;
                struct tm tmv;
                if (localtime_r(&cr, &tmv)) strftime(created_buf, sizeof(created_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                if (localtime_r(&up, &tmv)) strftime(updated_buf, sizeof(updated_buf), "%Y-%m-%d %H:%M:%S", &tmv);
                snprintf(resp.info, sizeof(resp.info),
                         "filename: %s\nowner: %s\ncreated: %s (%lld)\nupdated: %s (%lld)\nreaders: %s\nwriters: %s\n",
                         req.filename,
                         owner[0]?owner:"",
                         created_buf[0]?created_buf:"", created,
                         updated_buf[0]?updated_buf:"", updated,
                         readers[0]?readers:"",
                         writers[0]?writers:"");
                MsgHeader h = { .command = CMD_INFO_RESP, .payload_size = sizeof(resp) };
                send_all(nm_socket, &h, sizeof(h));
                send_all(nm_socket, &resp, sizeof(resp));
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
