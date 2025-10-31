#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <poll.h>

int nm_socket;
char username[MAX_USERNAME_LEN];
// Watchdog: exit immediately if NM socket is closed (no need to wait for next command)
static void* nm_disconnect_watchdog(void* arg) {
    (void)arg;
    struct pollfd pfd;
    pfd.fd = nm_socket;
    pfd.events = POLLIN | POLLERR | POLLHUP;
#ifdef POLLRDHUP
    pfd.events |= POLLRDHUP;
#endif
    while (1) {
        int rc = poll(&pfd, 1, -1); // block until event
        if (rc > 0) {
            if (pfd.revents & (POLLERR | POLLHUP
#ifdef POLLRDHUP
                               | POLLRDHUP
#endif
                               )) {
                fprintf(stderr, "\n[Client] Name Server disconnected. Exiting.\n");
                // Best-effort flush
                fflush(stdout);
                _exit(1);
            }
            if (pfd.revents & POLLIN) {
                // Something to read: check if it's a disconnect (read returns 0)
                char tmp;
                ssize_t n = recv(nm_socket, &tmp, 1, MSG_PEEK);
                if (n == 0) {
                    fprintf(stderr, "\n[Client] Name Server disconnected. Exiting.\n");
                    fflush(stdout);
                    _exit(1);
                }
                // else data is available; let main thread handle it
            }
        } else if (rc < 0) {
            // Poll failed; treat as fatal to avoid inconsistent state
            perror("poll");
            _exit(1);
        }
    }
    return NULL;
}

/**
 * @brief Thread to listen for responses/updates from the Name Server
 */
void* listen_to_nm(void* arg) {
    MsgHeader header;
    
    // Loop forever, waiting for responses from the NM
    while (true) {
        if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
            fprintf(stderr, "\n[Client] Connection to Name Server lost! Exiting.\n");
            exit(EXIT_FAILURE); // If NM is down, client can't function
        }

        printf("\n[Client] Received async command %d from Name Server.\n", header.command);
        
        // TODO: In the future, handle async updates
        // For now, just print the prompt again
        printf("%s> ", username);
        fflush(stdout);
    }
    return NULL;
}

// Helper function to remove trailing newline from fgets
void remove_newline(char* str) {
    str[strcspn(str, "\n")] = 0;
}

int main() {
    // --- Get Username ---
    printf("Enter your username: ");
    if (fgets(username, MAX_USERNAME_LEN, stdin) == NULL) {
        fprintf(stderr, "Error reading username.\n");
        return 1;
    }
    remove_newline(username);
    
    if (strlen(username) == 0) {
        fprintf(stderr, "Invalid username.\n");
        return 1;
    }

    // --- Connect to Name Server ---
    struct sockaddr_in nm_address;
    if ((nm_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("[Client] Socket creation error");
        return 1;
    }

    nm_address.sin_family = AF_INET;
    nm_address.sin_port = htons(NAME_SERVER_PORT);
    if (inet_pton(AF_INET, "127.0.0.1", &nm_address.sin_addr) <= 0) {
        perror("[Client] Invalid address");
        return 1;
    }

    printf("[Client] Connecting to Name Server...\n");
    if (connect(nm_socket, (struct sockaddr *)&nm_address, sizeof(nm_address)) < 0) {
        perror("[Client] Connection Failed");
        return 1;
    }
    printf("[Client] Connected to Name Server!\n");

    // --- Register with Name Server ---
    MsgHeader header;
    MsgRegisterClient reg_msg;
    
    header.command = CMD_REGISTER_CLIENT;
    header.payload_size = sizeof(MsgRegisterClient);
    strncpy(reg_msg.username, username, MAX_USERNAME_LEN);

    printf("[Client] Registering as '%s'...\n", username);
    if (!send_all(nm_socket, &header, sizeof(MsgHeader))) return 1;
    if (!send_all(nm_socket, &reg_msg, sizeof(MsgRegisterClient))) return 1;
    
    // --- Wait for ACK ---
    if (!recv_all(nm_socket, &header, sizeof(MsgHeader))) {
        fprintf(stderr, "[Client] Failed to receive ACK from NM.\n");
        return 1;
    }
    if (header.command == CMD_ACK) {
        printf("[Client] Registration successful!\n");
    } else {
        fprintf(stderr, "[Client] Registration failed.\n");
        return 1;
    }

    // --- Start watchdog to exit immediately if NM disconnects ---
    pthread_t watchdog_id;
    if (pthread_create(&watchdog_id, NULL, nm_disconnect_watchdog, NULL) != 0) {
        perror("[Client] Failed to start NM watchdog");
        return 1;
    }
    pthread_detach(watchdog_id);

    // --- MAIN REPL (Read-Eval-Print Loop) ---
    char line[1024];
    while (true) {
        printf("%s> ", username);
        fflush(stdout);
        
        if (fgets(line, sizeof(line), stdin) == NULL) {
            break; // EOF (e.g., Ctrl+D)
        }
        remove_newline(line);
        if (strlen(line) == 0) {
            continue;
        }

        // Parse command using strtok
        char* command = strtok(line, " ");

        if (command == NULL) {
            continue;
        }

        if (strcmp(command, "exit") == 0) {
            break;
        } else if (strcmp(command, "CREATE") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) {
                printf("[Client] Usage: CREATE <filename>\n");
                continue;
            }

            MsgHeader h = {0};
            MsgCreateFile payload = {0};
            h.command = CMD_CREATE_FILE;
            h.payload_size = sizeof(MsgCreateFile);
            strncpy(payload.filename, fname, MAX_FILENAME_LEN - 1);
            strncpy(payload.owner, username, MAX_USERNAME_LEN - 1);

            if (!send_all(nm_socket, &h, sizeof(h)) ||
                !send_all(nm_socket, &payload, sizeof(payload))) {
                fprintf(stderr, "[Client] Failed to send CREATE to NM (disconnected). Exiting.\n");
                break;
            }

            // Wait for NM response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                fprintf(stderr, "[Client] NM disconnected. Exiting.\n");
                break;
            }

            if (rh.command == CMD_ACK) {
                printf("[Client] File '%s' created.\n", fname);
            } else if (rh.command == CMD_ERROR) {
                MsgError err = {0};
                if (rh.payload_size == sizeof(MsgError) &&
                    recv_all(nm_socket, &err, sizeof(err))) {
                    printf("[Client] CREATE failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("[Client] CREATE failed (unknown error).\n");
                }
            } else {
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "VIEW") == 0) {
            // Send VIEW request to NM (no payload)
            MsgHeader h = {0};
            h.command = CMD_VIEW_FILES;
            h.payload_size = 0;

            if (!send_all(nm_socket, &h, sizeof(h))) {
                fprintf(stderr, "[Client] Failed to send VIEW to NM (disconnected). Exiting.\n");
                break;
            }

            // Await response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                fprintf(stderr, "[Client] NM disconnected. Exiting.\n");
                break;
            }

            if (rh.command == CMD_VIEW_FILES_RESP && rh.payload_size == sizeof(MsgViewFilesResponse)) {
                MsgViewFilesResponse resp = {0};
                if (recv_all(nm_socket, &resp, sizeof(resp))) {
                    // Print list (may be empty)
                    printf("%s", resp.file_list);
                } else {
                    printf("[Client] VIEW failed: payload read error.\n");
                }
            } else if (rh.command == CMD_ERROR) {
                MsgError err = {0};
                if (rh.payload_size == sizeof(MsgError) && recv_all(nm_socket, &err, sizeof(err))) {
                    printf("[Client] VIEW failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("[Client] VIEW failed (unknown error).\n");
                }
            } else {
                // Drain any payload if present
                size_t remaining = rh.payload_size;
                char drain[512];
                while (remaining > 0) {
                    size_t chunk = remaining > sizeof(drain) ? sizeof(drain) : remaining;
                    if (!recv_all(nm_socket, drain, chunk)) break;
                    remaining -= chunk;
                }
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "READ") == 0) {
            // Not implemented yet
            printf("[Client] Command '%s' not implemented yet.\n", command);
        } else {
            printf("[Client] Unknown command: %s\n", command);
        }
    }

    printf("[Client] Disconnecting...\n");
    close(nm_socket);
    return 0;
}
