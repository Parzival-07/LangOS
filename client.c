#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <poll.h>
#include <strings.h>

int nm_socket;
char username[MAX_USERNAME_LEN];

// Watchdog thread: exits the process as soon as the NM socket is closed,
// without waiting for the next command round-trip.
void* nm_watchdog(void* arg) {
    (void)arg;
    while (1) {
        struct pollfd pfd;
        pfd.fd = nm_socket;
        pfd.events = POLLIN | POLLHUP | POLLERR;
        pfd.revents = 0;

        int r = poll(&pfd, 1, 500); // 500ms heartbeat
        if (r < 0) {
            perror("[Client] poll");
            // On poll error, be conservative and exit
            fprintf(stderr, "\n[Client] NM watchdog exiting due to poll error.\n");
            _exit(1);
        }
        if (r == 0) {
            continue; // timeout, loop again
        }
        if (pfd.revents & (POLLHUP | POLLERR)) {
            fprintf(stderr, "\n[Client] Name Server connection closed. Exiting.\n");
            _exit(1);
        }
        if (pfd.revents & POLLIN) {
            // Non-consuming peek to detect EOF (0 means closed)
            char ch;
            ssize_t n = recv(nm_socket, &ch, 1, MSG_PEEK);
            if (n == 0) {
                fprintf(stderr, "\n[Client] Name Server disconnected. Exiting.\n");
                _exit(1);
            }
            // n > 0 means data pending; ignore (main thread will read it)
        }
    }
    return NULL;
}

// Removed broken listen_to_nm placeholder from merge; NM interactions are synchronous in the REPL.

// Helper function to remove trailing newline from fgets
void remove_newline(char* str) {
    str[strcspn(str, "\n")] = 0;
}

// Portable strdup to avoid missing prototype issues on some platforms
static char* safe_strdup(const char* s) {
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char* p = (char*)malloc(n);
    if (!p) return NULL;
    memcpy(p, s, n);
    return p;
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

    // --- Start NM disconnect watchdog (proactive exit on NM down) ---
    pthread_t wd_thread_id;
    if (pthread_create(&wd_thread_id, NULL, nm_watchdog, NULL) != 0) {
        perror("[Client] Failed to create NM watchdog thread");
        // Non-fatal: client will still exit on next NM interaction
    } else {
        pthread_detach(wd_thread_id);
    }

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
                fprintf(stderr, "[Client] Failed to send CREATE to NM.\n");
                continue;
            }

            // Wait for NM response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                fprintf(stderr, "[Client] No response from NM.\n");
                break; // Exit immediately on NM disconnect
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
        } else if (strcmp(command, "DELETE") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) {
                printf("[Client] Usage: DELETE <filename>\n");
                continue;
            }

            MsgHeader h = {0};
            MsgDeleteFile payload = {0};
            h.command = CMD_DELETE_FILE;
            h.payload_size = sizeof(MsgDeleteFile);
            strncpy(payload.filename, fname, MAX_FILENAME_LEN - 1);
            // requester will be set by NM; we can set it here for completeness
            strncpy(payload.requester, username, MAX_USERNAME_LEN - 1);

            if (!send_all(nm_socket, &h, sizeof(h)) ||
                !send_all(nm_socket, &payload, sizeof(payload))) {
                fprintf(stderr, "[Client] Failed to send DELETE to NM.\n");
                continue;
            }

            // Wait for NM response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                fprintf(stderr, "[Client] No response from NM.\n");
                break; // Exit immediately on NM disconnect
            }

            if (rh.command == CMD_ACK) {
                printf("[Client] File '%s' deleted.\n", fname);
            } else if (rh.command == CMD_ERROR) {
                MsgError err = {0};
                if (rh.payload_size == sizeof(MsgError) &&
                    recv_all(nm_socket, &err, sizeof(err))) {
                    printf("[Client] DELETE failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("[Client] DELETE failed (unknown error).\n");
                }
            } else {
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "VIEW") == 0) {
            // VIEW flags: -a (all), -l (long listing). Accept combinations: -a, -l, -al, -la
            // Default behavior: show all files so users can discover files (even without access)
            int show_all = 1, long_list = 0;
            char* flag = strtok(NULL, " ");
            if (flag) {
                if (strcmp(flag, "-a") == 0) { show_all = 1; }
                else if (strcmp(flag, "-l") == 0) { long_list = 1; }
                else if (strcmp(flag, "-al") == 0 || strcmp(flag, "-la") == 0) { show_all = 1; long_list = 1; }
                else {
                    printf("[Client] Usage: VIEW [-a|-l|-al]\n");
                }
            }

            MsgViewFilesRequest rq = {0};
            rq.show_all = show_all;
            rq.long_list = long_list;
            strncpy(rq.requester, username, MAX_USERNAME_LEN - 1);
            MsgHeader h = {0};
            h.command = CMD_VIEW_FILES;
            h.payload_size = sizeof(rq);

            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) {
                fprintf(stderr, "[Client] Failed to send VIEW to NM.\n");
                continue;
            }

            // Wait for NM response
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                fprintf(stderr, "[Client] No response from NM.\n");
                break; // Exit immediately on NM disconnect
            }

            if (rh.command == CMD_VIEW_FILES_RESP && rh.payload_size == sizeof(MsgViewFilesResponse)) {
                MsgViewFilesResponse resp = {0};
                if (!recv_all(nm_socket, &resp, sizeof(resp))) {
                    fprintf(stderr, "[Client] Failed to read VIEW payload.\n");
                    break; // Treat as NM disconnect and exit
                }
                if (resp.file_list[0] == '\0') {
                    printf("[Client] No files available.\n");
                } else {
                    printf("%s", resp.file_list);
                }
            } else if (rh.command == CMD_ERROR) {
                MsgError err = {0};
                if (rh.payload_size == sizeof(MsgError) && recv_all(nm_socket, &err, sizeof(err))) {
                    printf("[Client] VIEW failed (%d): %s\n", err.code, err.message);
                } else {
                    printf("[Client] VIEW failed (unknown error).\n");
                }
            } else {
                // Drain any unexpected payload
                size_t remaining = rh.payload_size;
                char drain[512];
                while (remaining > 0) {
                    size_t chunk = remaining > sizeof(drain) ? sizeof(drain) : remaining;
                    if (!recv_all(nm_socket, drain, chunk)) break;
                    remaining -= chunk;
                }
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "WRITE") == 0) {
            char* fname = strtok(NULL, " ");
            char* sidx_s = strtok(NULL, " ");
            if (!fname || !sidx_s) {
                printf("[Client] Usage: WRITE <filename> <sentence_number>\n");
                continue;
            }
            int sidx = atoi(sidx_s);
            if (sidx < 0) { printf("[Client] Invalid sentence index.\n"); continue; }

            // Resolve SS address for this file via NM (reuse READ routing)
            MsgHeader h = (MsgHeader){0};
            MsgReadFile req = (MsgReadFile){0};
            h.command = CMD_READ_FILE;
            h.payload_size = sizeof(req);
            strncpy(req.filename, fname, MAX_FILENAME_LEN - 1);
            strncpy(req.requester, username, MAX_USERNAME_LEN - 1);
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &req, sizeof(req))) {
                fprintf(stderr, "[Client] Failed to query NM for file location.\n");
                continue;
            }
            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) { fprintf(stderr, "[Client] NM no response.\n"); continue; }
            if (rh.command != CMD_READ_FILE_RESP || rh.payload_size != sizeof(MsgReadFileResponse)) {
                // Drain
                size_t rem = rh.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(nm_socket, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected NM response (%d).\n", rh.command);
                continue;
            }
            MsgReadFileResponse addr = {0};
            if (!recv_all(nm_socket, &addr, sizeof(addr))) { fprintf(stderr, "[Client] Failed reading location.\n"); continue; }
            if (!addr.found) { printf("[Client] File not found.\n"); continue; }

            // Connect to SS (header-based protocol)
            int ss_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (ss_fd < 0) { perror("[Client] socket"); continue; }
            struct sockaddr_in ss_addr; memset(&ss_addr, 0, sizeof(ss_addr));
            ss_addr.sin_family = AF_INET; ss_addr.sin_port = htons((uint16_t)addr.ss_port);
            if (inet_pton(AF_INET, addr.ss_ip, &ss_addr.sin_addr) <= 0) { perror("[Client] inet_pton"); close(ss_fd); continue; }
            if (connect(ss_fd, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) { perror("[Client] connect SS"); close(ss_fd); continue; }

            // Acquire lock: CMD_WRITE_BEGIN
            MsgWriteBegin begin = (MsgWriteBegin){0}; strncpy(begin.filename, fname, MAX_FILENAME_LEN - 1); begin.sentence_index = sidx; strncpy(begin.requester, username, MAX_USERNAME_LEN - 1);
            MsgHeader bh = { .command = CMD_WRITE_BEGIN, .payload_size = sizeof(begin) };
            if (!send_all(ss_fd, &bh, sizeof(bh)) || !send_all(ss_fd, &begin, sizeof(begin))) { fprintf(stderr, "[Client] Failed to send WRITE_BEGIN.\n"); close(ss_fd); continue; }
            MsgHeader brh; if (!recv_all(ss_fd, &brh, sizeof(brh))) { fprintf(stderr, "[Client] No response for WRITE_BEGIN.\n"); close(ss_fd); continue; }
            if (brh.command == CMD_ERROR && brh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(ss_fd, &err, sizeof(err)); printf("[Client] WRITE_BEGIN failed (%d): %s\n", err.code, err.message); close(ss_fd); continue;
            } else if (brh.command != CMD_ACK) {
                // Drain
                size_t rem = brh.payload_size; char drain[256]; while (rem > 0) { size_t chunk = rem > sizeof(drain)?sizeof(drain):rem; if (!recv_all(ss_fd, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected response to WRITE_BEGIN (%d).\n", brh.command); close(ss_fd); continue;
            }

            // Fetch file content via header-based READ to build the working sentence
            MsgHeader rqh = (MsgHeader){ .command = CMD_READ_FILE, .payload_size = sizeof(MsgReadFile) };
            if (!send_all(ss_fd, &rqh, sizeof(rqh)) || !send_all(ss_fd, &req, sizeof(req))) { fprintf(stderr, "[Client] Failed to request file from SS.\n"); close(ss_fd); continue; }
            MsgHeader rrh; if (!recv_all(ss_fd, &rrh, sizeof(rrh))) { fprintf(stderr, "[Client] No READ response from SS.\n"); close(ss_fd); continue; }
            if (rrh.command == CMD_ERROR && rrh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(ss_fd, &err, sizeof(err)); printf("[Client] READ failed (%d): %s\n", err.code, err.message); close(ss_fd); continue; }
            if (rrh.command != CMD_ACK || rrh.payload_size < 0) { printf("[Client] Unexpected READ response (%d).\n", rrh.command); close(ss_fd); continue; }
            int fsize = rrh.payload_size; char* filebuf = (char*)malloc((size_t)fsize + 1); if (!filebuf) { fprintf(stderr, "[Client] OOM.\n"); close(ss_fd); continue; }
            if (fsize > 0 && !recv_all(ss_fd, filebuf, (size_t)fsize)) { fprintf(stderr, "[Client] Failed to receive file data.\n"); free(filebuf); close(ss_fd); continue; }
            filebuf[fsize] = '\0';

            // Parse sentences: split on .!? including trailing whitespace
            // Locate target sentence string boundaries
            int current = 0; size_t i = 0, n = (size_t)fsize; size_t s_begin = 0, s_end = 0; int found = 0;
            while (i < n) {
                size_t start = i;
                while (i < n && filebuf[i] != '.' && filebuf[i] != '!' && filebuf[i] != '?') i++;
                if (i < n && (filebuf[i] == '.' || filebuf[i] == '!' || filebuf[i] == '?')) {
                    i++;
                    while (i < n && (filebuf[i] == ' ' || filebuf[i] == '\n' || filebuf[i] == '\t' || filebuf[i] == '\r')) i++;
                }
                size_t end = i;
                if (current == sidx) { s_begin = start; s_end = end; found = 1; break; }
                current++;
            }
            // If not found but it's exactly at the end (append new sentence), allow editing empty sentence
            if (!found && sidx == current) { s_begin = s_end = n; found = 1; }
            if (!found) {
                printf("[Client] Sentence %d not found.\n", sidx);
                // release lock
                MsgWriteDone done = (MsgWriteDone){0}; strncpy(done.filename, fname, MAX_FILENAME_LEN-1); done.sentence_index = sidx;
                MsgHeader dh = { .command = CMD_WRITE_DONE, .payload_size = sizeof(done) };
                send_all(ss_fd, &dh, sizeof(dh)); send_all(ss_fd, &done, sizeof(done));
                free(filebuf); close(ss_fd);
                continue;
            }

            // Extract sentence into working words
            size_t slen = (s_end > s_begin ? (s_end - s_begin) : 0);
            char* sentence = (char*)malloc(slen + 1);
            if (!sentence) { fprintf(stderr, "[Client] OOM.\n"); free(filebuf); close(ss_fd); continue; }
            memcpy(sentence, filebuf + s_begin, slen); sentence[slen] = '\0';
            free(filebuf);

            // Tokenize by spaces (simple)
            char** words = NULL; int wcount = 0; size_t cap = 0;
            char* tokbuf = safe_strdup(sentence);
            if (!tokbuf) { fprintf(stderr, "[Client] OOM.\n"); free(sentence); close(ss_fd); continue; }
            char* saveptr = NULL; char* t = strtok_r(tokbuf, " \t\r\n", &saveptr);
            while (t) {
                if (wcount == (int)cap) { cap = cap ? cap*2 : 8; char** nw = realloc(words, cap * sizeof(char*)); if (!nw) { fprintf(stderr, "[Client] OOM.\n"); break; } words = nw; }
                char* dup = safe_strdup(t);
                if (!dup) { fprintf(stderr, "[Client] OOM.\n"); break; }
                words[wcount++] = dup;
                t = strtok_r(NULL, " \t\r\n", &saveptr);
            }
            free(tokbuf);

            // Show current sentence words (1-based)
            if (wcount > 0) {
                printf("[Client] Current sentence words (1-based):\n");
                for (int iw = 0; iw < wcount; iw++) {
                    printf("  %d: %s\n", iw+1, words[iw]);
                }
            } else {
                printf("[Client] Current sentence is empty.\n");
            }
            printf("[Client] Enter one of the following then ETIRW:\n");
            printf("[Client]   '1 <sentence>' to replace this sentence\n");
            printf("[Client]   '2 <sentence>' to insert a new sentence after this one\n");
            printf("[Client]   (optional) 'INDEX 1: <sentence>' or 'INDEX 2: <sentence>' also supported)\n");
            printf("[Client] Type 'ETIRW' to finish.\n");

            int sentence_mode = 0; // 0=word-edit, 1=replace(current), 2=insert(after)
            char* sentence_text = NULL;
            while (1) {
                printf("WRITE> "); fflush(stdout);
                char wline[4096]; if (!fgets(wline, sizeof(wline), stdin)) { printf("\n[Client] Input ended.\n"); break; }
                wline[strcspn(wline, "\n")] = 0;
                if (strcmp(wline, "ETIRW") == 0) break;
                // Sentence-level operations: INDEX n: text  (n must be 1 or 2)
                char wlcopy[4096]; strncpy(wlcopy, wline, sizeof(wlcopy)-1); wlcopy[sizeof(wlcopy)-1] = 0;
                char* pz = wlcopy; while (*pz==' ') pz++;
                if (strncasecmp(pz, "INDEX", 5) == 0 || (*pz=='S' || *pz=='s')) {
                    // advance past keyword if present
                    if (strncasecmp(pz, "INDEX", 5) == 0) pz += 5; else pz += 1;
                    while (*pz==' ') pz++;
                    // parse number possibly followed by ':'
                    char* ep = pz; long n = strtol(pz, &ep, 10);
                    while (*ep==' ' || *ep==':') ep++;
                    if (n != 1 && n != 2) { printf("[Client] Sentence index must be 1 (replace) or 2 (insert-after).\n"); continue; }
                    if (sentence_text) { free(sentence_text); sentence_text = NULL; }
                    sentence_text = safe_strdup(ep ? ep : "");
                    sentence_mode = (int)n;
                    printf("[Client] Queued sentence %s with text: '%s'\n", (sentence_mode==1?"replacement":"insertion"), sentence_text);
                    continue;
                }
                // Numeric entry: interpret strictly as sentence-level (1 or 2)
                char* p = wline; while (*p==' ') p++;
                if (!*p) continue;
                char* endptr = NULL; long idx1 = strtol(p, &endptr, 10);
                if (p == endptr) { printf("[Client] Expected '<index> <sentence>'.\n"); continue; }
                while (*endptr==' ') endptr++;
                char* content = endptr; if (!content) content = "";
                if (idx1 != 1 && idx1 != 2) { printf("[Client] Only sentence indices 1 (replace) or 2 (insert-after) are allowed.\n"); continue; }
                if (sentence_text) { free(sentence_text); sentence_text = NULL; }
                sentence_text = safe_strdup(content);
                sentence_mode = (int)idx1;
                printf("[Client] Queued sentence %s with text: '%s'\n", (sentence_mode==1?"replacement":"insertion"), sentence_text);
                continue;
            }

            // No word-level editing output is used; we operate at sentence level only.
            char* final_sentence = NULL; // unused unless we fall back; kept for cleanup path

            // If user made no changes, cancel without modifying the file
            if (sentence_mode == 0 && (!sentence_text || sentence_text[0] == '\0')) {
                printf("[Client] No changes; canceling WRITE.\n");
                // Release lock cleanly
                MsgWriteDone cancel = (MsgWriteDone){0};
                strncpy(cancel.filename, fname, MAX_FILENAME_LEN - 1);
                cancel.sentence_index = sidx;
                strncpy(cancel.requester, username, MAX_USERNAME_LEN - 1);
                MsgHeader ch = { .command = CMD_WRITE_DONE, .payload_size = sizeof(cancel) };
                send_all(ss_fd, &ch, sizeof(ch));
                send_all(ss_fd, &cancel, sizeof(cancel));
                // best-effort read of response
                MsgHeader crh; recv_all(ss_fd, &crh, sizeof(crh));
                // Cleanup and exit
                for (int iw2 = 0; iw2 < wcount; iw2++) free(words[iw2]); if (words) free(words);
                free(sentence);
                if (final_sentence) free(final_sentence);
                if (sentence_text) free(sentence_text);
                close(ss_fd);
                continue;
            }

            // Apply write (sentence-level op takes precedence if provided)
            MsgWriteFile w = (MsgWriteFile){0}; strncpy(w.filename, fname, MAX_FILENAME_LEN - 1); strncpy(w.requester, username, MAX_USERNAME_LEN - 1);
            if (sentence_mode == 1 || sentence_mode == 2) {
                // enforce bounds: only 1 (replace current) or 2 (insert after current)
                if (sentence_mode == 1) { w.sentence_index = sidx; }
                else { w.sentence_index = sidx + 1; }
                const char* st = sentence_text ? sentence_text : "";
                strncpy(w.replacement, st, sizeof(w.replacement)-1);
            } else {
                w.sentence_index = sidx;
                if (final_sentence) { strncpy(w.replacement, final_sentence, sizeof(w.replacement)-1); } else { w.replacement[0] = '\0'; }
            }
            MsgHeader wh = { .command = CMD_WRITE_FILE, .payload_size = sizeof(w) };
            if (!send_all(ss_fd, &wh, sizeof(wh)) || !send_all(ss_fd, &w, sizeof(w))) { fprintf(stderr, "[Client] Failed to send WRITE.\n"); }
            MsgHeader wr; if (!recv_all(ss_fd, &wr, sizeof(wr))) { fprintf(stderr, "[Client] No response to WRITE.\n"); }
            if (wr.command == CMD_ERROR && wr.payload_size == sizeof(MsgError)) { MsgError err; recv_all(ss_fd, &err, sizeof(err)); printf("[Client] WRITE failed (%d): %s\n", err.code, err.message); }
            else if (wr.command != CMD_ACK) { printf("[Client] Unexpected response to WRITE (%d).\n", wr.command); }
            else { printf("[Client] WRITE applied.\n"); }

            // Release lock
            MsgWriteDone done = (MsgWriteDone){0}; strncpy(done.filename, fname, MAX_FILENAME_LEN - 1); done.sentence_index = sidx; strncpy(done.requester, username, MAX_USERNAME_LEN - 1);
            MsgHeader dh = { .command = CMD_WRITE_DONE, .payload_size = sizeof(done) };
            send_all(ss_fd, &dh, sizeof(dh)); send_all(ss_fd, &done, sizeof(done));
            MsgHeader drh; if (recv_all(ss_fd, &drh, sizeof(drh))) {
                if (drh.command == CMD_ERROR && drh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(ss_fd, &err, sizeof(err)); printf("[Client] WRITE_DONE failed (%d): %s\n", err.code, err.message); }
            }

            // Cleanup
            if (final_sentence) free(final_sentence);
            if (sentence_text) free(sentence_text);
            for (int iw2 = 0; iw2 < wcount; iw2++) free(words[iw2]); if (words) free(words);
            free(sentence);
            close(ss_fd);
        } else if (strcmp(command, "ADDACCESS") == 0) {
            // Spec: ADDACCESS -R|-W <filename> <username>
            // Backward compat: ADDACCESS <filename> <username> <R|W>
            char* flag = strtok(NULL, " ");
            char* fname = NULL; char* target = NULL; int is_writer = -1;
            if (flag && flag[0] == '-') {
                // New spec ordering
                if (strcasecmp(flag, "-R") == 0) is_writer = 0;
                else if (strcasecmp(flag, "-W") == 0) is_writer = 1;
                fname = strtok(NULL, " ");
                target = strtok(NULL, " ");
            } else {
                // Legacy ordering
                fname = flag;
                target = strtok(NULL, " ");
                char* mode = strtok(NULL, " ");
                if (mode) {
                    if (mode[0]=='R' || mode[0]=='r') is_writer = 0;
                    else if (mode[0]=='W' || mode[0]=='w') is_writer = 1;
                }
            }
            if (!fname || !target || !(is_writer==0 || is_writer==1)) {
                printf("[Client] Usage: ADDACCESS -R|-W <filename> <username>\n");
                continue;
            }
            MsgAccessChange ac = (MsgAccessChange){0};
            strncpy(ac.filename, fname, MAX_FILENAME_LEN-1);
            strncpy(ac.target, target, MAX_USERNAME_LEN-1);
            strncpy(ac.requester, username, MAX_USERNAME_LEN-1);
            ac.is_writer = is_writer;
            MsgHeader h = { .command = CMD_ADD_ACCESS, .payload_size = sizeof(ac) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &ac, sizeof(ac))) {
                fprintf(stderr, "[Client] Failed to send ADDACCESS.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { fprintf(stderr, "[Client] NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) printf("[Client] Access added.\n");
            else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] ADDACCESS failed (%d): %s\n", err.code, err.message); }
            else printf("[Client] Unexpected response (%d).\n", rh.command);
        } else if (strcmp(command, "REMACCESS") == 0) {
            // Spec: REMACCESS <filename> <username>
            char* fname = strtok(NULL, " ");
            char* target = strtok(NULL, " ");
            if (!fname || !target) { printf("[Client] Usage: REMACCESS <filename> <username>\n"); continue; }
            MsgAccessChange ac = (MsgAccessChange){0};
            strncpy(ac.filename, fname, MAX_FILENAME_LEN-1);
            strncpy(ac.target, target, MAX_USERNAME_LEN-1);
            strncpy(ac.requester, username, MAX_USERNAME_LEN-1);
            ac.is_writer = 0; // ignored by server on removal
            MsgHeader h = { .command = CMD_REM_ACCESS, .payload_size = sizeof(ac) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &ac, sizeof(ac))) {
                fprintf(stderr, "[Client] Failed to send REMACCESS.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { fprintf(stderr, "[Client] NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) printf("[Client] Access removed.\n");
            else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) { MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] REMACCESS failed (%d): %s\n", err.code, err.message); }
            else printf("[Client] Unexpected response (%d).\n", rh.command);
        } else if (strcmp(command, "INFO") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) { printf("[Client] Usage: INFO <filename>\n"); continue; }
            MsgInfoRequest rq = (MsgInfoRequest){0};
            strncpy(rq.filename, fname, MAX_FILENAME_LEN-1);
            MsgHeader h = { .command = CMD_INFO, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) {
                fprintf(stderr, "[Client] Failed to send INFO.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { fprintf(stderr, "[Client] NM disconnected.\n"); break; }
            if (rh.command == CMD_INFO_RESP && rh.payload_size == sizeof(MsgInfoResponse)) {
                MsgInfoResponse resp; if (recv_all(nm_socket, &resp, sizeof(resp))) { printf("%s\n", resp.info); }
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] INFO failed (%d): %s\n", err.code, err.message);
            } else {
                // Drain unexpected
                size_t rem = rh.payload_size; char drain[256]; while (rem > 0) { size_t chunk = rem > sizeof(drain)?sizeof(drain):rem; if (!recv_all(nm_socket, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected response (%d).\n", rh.command);
            }
        } else if (strcmp(command, "UNDO") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) { printf("[Client] Usage: UNDO <filename>\n"); continue; }
            MsgUndoRequest rq = (MsgUndoRequest){0};
            strncpy(rq.filename, fname, MAX_FILENAME_LEN-1);
            strncpy(rq.requester, username, MAX_USERNAME_LEN-1);
            MsgHeader h = { .command = CMD_UNDO, .payload_size = sizeof(rq) };
            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &rq, sizeof(rq))) {
                fprintf(stderr, "[Client] Failed to send UNDO.\n");
                continue;
            }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { fprintf(stderr, "[Client] NM disconnected.\n"); break; }
            if (rh.command == CMD_ACK) {
                printf("[Client] Undo applied to '%s'.\n", fname);
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(nm_socket, &err, sizeof(err));
                printf("[Client] UNDO failed (%d): %s\n", err.code, err.message);
            } else {
                // Drain unexpected response
                size_t rem = rh.payload_size; char drain[256];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(nm_socket, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected response to UNDO (%d).\n", rh.command);
            }
        } else if (strcmp(command, "READ") == 0) {
            char* fname = strtok(NULL, " ");
            if (!fname) {
                printf("[Client] Usage: READ <filename>\n");
                continue;
            }

            // Ask NM for SS address owning this file
            MsgHeader h = (MsgHeader){0};
            MsgReadFile req = (MsgReadFile){0};
            h.command = CMD_READ_FILE;
            h.payload_size = sizeof(req);
            strncpy(req.filename, fname, MAX_FILENAME_LEN - 1);
            strncpy(req.requester, username, MAX_USERNAME_LEN - 1);

            if (!send_all(nm_socket, &h, sizeof(h)) || !send_all(nm_socket, &req, sizeof(req))) {
                fprintf(stderr, "[Client] Failed to send READ to NM.\n");
                continue;
            }

            MsgHeader rh;
            if (!recv_all(nm_socket, &rh, sizeof(rh))) {
                fprintf(stderr, "[Client] No response from NM.\n");
                break; // Exit immediately on NM disconnect
            }
            if (rh.command != CMD_READ_FILE_RESP || rh.payload_size != sizeof(MsgReadFileResponse)) {
                // Drain unexpected payload
                size_t rem = rh.payload_size; char drain[512];
                while (rem > 0) { size_t chunk = rem > sizeof(drain) ? sizeof(drain) : rem; if (!recv_all(nm_socket, drain, chunk)) break; rem -= chunk; }
                printf("[Client] Unexpected response (%d).\n", rh.command);
                continue;
            }

            MsgReadFileResponse addr = {0};
            if (!recv_all(nm_socket, &addr, sizeof(addr))) {
                fprintf(stderr, "[Client] Failed to read READ response payload.\n");
                break; // Treat as NM disconnect and exit
            }
            if (!addr.found || addr.ss_port <= 0 || addr.ss_ip[0] == '\0') {
                printf("[Client] READ failed: file not found.\n");
                continue;
            }

            // Connect directly to SS
            int ss_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (ss_fd < 0) { perror("[Client] socket"); continue; }
            struct sockaddr_in ss_addr; memset(&ss_addr, 0, sizeof(ss_addr));
            ss_addr.sin_family = AF_INET;
            ss_addr.sin_port = htons((uint16_t)addr.ss_port);
            if (inet_pton(AF_INET, addr.ss_ip, &ss_addr.sin_addr) <= 0) {
                perror("[Client] inet_pton"); close(ss_fd); continue;
            }
            if (connect(ss_fd, (struct sockaddr*)&ss_addr, sizeof(ss_addr)) < 0) {
                perror("[Client] connect to SS"); close(ss_fd); continue;
            }

            // Send username and filename on two lines for ACL-aware simple protocol
            size_t ulen = strnlen(username, MAX_USERNAME_LEN);
            size_t flen = strnlen(fname, MAX_FILENAME_LEN);
            if (!send_all(ss_fd, username, ulen) || !send_all(ss_fd, "\n", 1) ||
                !send_all(ss_fd, fname, flen) || !send_all(ss_fd, "\n", 1)) {
                fprintf(stderr, "[Client] Failed to send filename to SS.\n");
                close(ss_fd); continue;
            }

            // Read all content until EOF and print
            char buf[4096];
            ssize_t n;
            while ((n = recv(ss_fd, buf, sizeof(buf), 0)) > 0) {
                fwrite(buf, 1, (size_t)n, stdout);
            }
            if (n < 0) perror("[Client] recv from SS");
            // Ensure a trailing newline for cleanliness
            if (flen > 0) printf("\n");
            close(ss_fd);
        } else if (strcmp(command, "LIST") == 0) {
            // Request user list from Name Server
            MsgHeader h = (MsgHeader){ .command = CMD_LIST_USERS, .payload_size = 0 };
            if (!send_all(nm_socket, &h, sizeof(h))) { fprintf(stderr, "[Client] Failed to send LIST.\n"); continue; }
            MsgHeader rh; if (!recv_all(nm_socket, &rh, sizeof(rh))) { fprintf(stderr, "[Client] NM disconnected.\n"); break; }
            if (rh.command == CMD_LIST_USERS_RESP && rh.payload_size == sizeof(MsgUsersListResponse)) {
                MsgUsersListResponse resp = {0};
                if (!recv_all(nm_socket, &resp, sizeof(resp))) { fprintf(stderr, "[Client] Failed to read LIST payload.\n"); break; }
                if (resp.users[0] == '\0') { printf("[Client] No users currently registered.\n"); }
                else { printf("%s", resp.users); }
            } else if (rh.command == CMD_ERROR && rh.payload_size == sizeof(MsgError)) {
                MsgError err; recv_all(nm_socket, &err, sizeof(err)); printf("[Client] LIST failed (%d): %s\n", err.code, err.message);
            } else {
                // Drain any unexpected payload
                size_t rem = rh.payload_size; char drain[256]; while (rem > 0) { size_t ch = rem>sizeof(drain)?sizeof(drain):rem; if (!recv_all(nm_socket, drain, ch)) break; rem -= ch; }
                printf("[Client] Unexpected response to LIST (%d).\n", rh.command);
            }
        } else {
            printf("[Client] Unknown command: %s\n", command);
        }
    }

    printf("[Client] Disconnecting...\n");
    close(nm_socket);
    return 0;
}
