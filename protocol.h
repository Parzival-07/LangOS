#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string.h> // for memset, strcpy, etc.
#include <unistd.h> // for read, write
#include <sys/socket.h> // for socket operations
#include <stdbool.h> // for bool, true, false
#include <stdio.h> // for perror

// --- CONFIGURATION ---

#define NAME_SERVER_PORT 8000
#define MAX_USERNAME_LEN 64
#define MAX_FILENAME_LEN 256
#define MAX_ERROR_MSG_LEN 256
#define MAX_IP_LEN 64

// --- COMMAND DEFINITIONS ---
// This is the "language" our servers speak.
typedef enum {
    // Registration commands
    CMD_REGISTER_SS = 100,
    CMD_REGISTER_CLIENT = 101,

    // Acknowledgement / Error
    CMD_ACK = 200,    // General success
    CMD_ERROR = 201,  // General failure

    // File operations
    CMD_CREATE_FILE = 300,
    CMD_DELETE_FILE = 301,

    // View/listing operations
    // Client <-> NM
    CMD_VIEW_FILES = 310,
    CMD_VIEW_FILES_RESP = 311,

    // NM <-> SS (internal listing used by NM to refresh registry)
    CMD_SS_LIST_FILES = 320,
    CMD_SS_LIST_FILES_RESP = 321,

    // READ routing and response
    // Client <-> NM
    CMD_READ_FILE = 330,
    CMD_READ_FILE_RESP = 331
} CommandCode;

// --- DATA STRUCTURES ---
// We use fixed-size structs for simple network communication.

// Header sent before EVERY message
typedef struct {
    CommandCode command;
    int payload_size; // Size of the struct *following* this header
} MsgHeader;

// Data for CMD_REGISTER_SS
// SS -> NM
typedef struct {
    int client_listen_port; // The port this SS will open for clients
} MsgRegisterSS;

// Data for CMD_REGISTER_CLIENT
// Client -> NM
typedef struct {
    char username[MAX_USERNAME_LEN];
} MsgRegisterClient;

// Data for CMD_ERROR
// NM -> Client or SS
typedef struct {
    int  code;                       // error code (e.g., HTTP-ish or errno-ish)
    char message[MAX_ERROR_MSG_LEN]; // human readable message
} MsgError;

// Data for CMD_CREATE_FILE
// Client -> NM
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char owner[MAX_USERNAME_LEN];
} MsgCreateFile;

// Data for CMD_DELETE_FILE
// Client -> NM -> SS
typedef struct {
    char filename[MAX_FILENAME_LEN];
} MsgDeleteFile;

// Data for CMD_VIEW_FILES_RESP
// NM -> Client: newline-separated list of filenames
typedef struct {
    char file_list[16384];
} MsgViewFilesResponse;

// Data for CMD_SS_LIST_FILES_RESP
// SS -> NM: newline-separated list of filenames on that SS
typedef struct {
    char files[16384];
} MsgSSFileListResponse;

// Data for CMD_READ_FILE
// Client -> NM
typedef struct {
    char filename[MAX_FILENAME_LEN];
} MsgReadFile;

// Data for CMD_READ_FILE_RESP
// NM -> Client: IP/port of SS to connect to, and found flag
typedef struct {
    int found;                 // 1 if found, 0 otherwise
    char ss_ip[MAX_IP_LEN];    // IPv4 string (e.g., "127.0.0.1")
    int ss_port;               // SS client listen port
} MsgReadFileResponse;


// --- HELPER FUNCTIONS ---
// These ensure all data is sent/received reliably,
// handling cases where TCP sends/receives data in chunks.

/**
 * @brief Reliably sends a buffer of a specific size over a socket.
 * @param socket_fd The socket file descriptor.
 * @param buffer The data to send.
 * @param size The number of bytes to send.
 * @return true on success, false on failure.
 */
static inline bool send_all(int socket_fd, const void* buffer, size_t size) {
    const char* ptr = (const char*)buffer;
    size_t total_sent = 0;
    while (total_sent < size) {
        ssize_t sent = send(socket_fd, ptr + total_sent, size - total_sent, 0);
        if (sent <= 0) {
            // 0 means connection closed, -1 is an error
            perror("send_all");
            return false;
        }
        total_sent += (size_t)sent;
    }
    return true;
}

/**
 * @brief Reliably receives a specific number of bytes from a socket.
 * @param socket_fd The socket file descriptor.
 * @param buffer The buffer to fill.
 * @param size The number of bytes to receive.
 * @return true on success, false on failure (e.g., connection closed).
 */
static inline bool recv_all(int socket_fd, void* buffer, size_t size) {
    char* ptr = (char*)buffer;
    size_t total_received = 0;
    while (total_received < size) {
        ssize_t received = recv(socket_fd, ptr + total_received, size - total_received, 0);
        if (received <= 0) {
            // 0 means connection closed, -1 is an error
            if (received == 0) {
                // Connection closed gracefully
                return false;
            }
            perror("recv_all");
            return false;
        }
        total_received += (size_t)received;
    }
    return true;
}

#endif // PROTOCOL_H