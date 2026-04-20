/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    int stop_requested;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    // Wait if the buffer is full
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    
    // Add item to the array
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    
    // Wake up the consumer!
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    // Wait if the buffer is empty
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    
    // Remove item from the array
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    
    // Wake up producers!
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */

// Struct for the producer thread
typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_ctx_t;

// PRODUCER: Reads the pipe from the container and pushes to the buffer
void *producer_thread(void *arg) {
    producer_ctx_t *pctx = (producer_ctx_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t bytes_read;

    while ((bytes_read = read(pctx->read_fd, buf, sizeof(buf) - 1)) > 0) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        snprintf(item.container_id, sizeof(item.container_id), "%s", pctx->container_id);
        item.length = bytes_read;
        memcpy(item.data, buf, bytes_read);
        
        bounded_buffer_push(pctx->buffer, &item);
        printf("[Logger Producer] Pushed log chunk for %s\n", pctx->container_id);
    }
    
    close(pctx->read_fd);
    free(pctx);
    return NULL;
}

// CONSUMER: Pops from the buffer and writes to the hard drive
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    
    mkdir(LOG_DIR, 0755); // Ensure the logs/ folder exists

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char filepath[PATH_MAX];
        snprintf(filepath, sizeof(filepath), "%s/%s.log", LOG_DIR, item.container_id);
        
        int fd = open(filepath, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            if (write(fd, item.data, item.length) < 0) {
                perror("[Logger Consumer] File write failed");
            }
            close(fd);
            printf("[Logger Consumer] Wrote chunk to %s.log\n", item.container_id);
        }
    }
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *config = (child_config_t *)arg;

    // 1. Isolate the filesystem so the container only sees its own rootfs
    if (chroot(config->rootfs) != 0) {
        perror("[Container] chroot failed");
        return 1;
    }
    
    // Move to the new root directory
    if (chdir("/") != 0) {
        perror("[Container] chdir failed");
        return 1;
    }

    // 2. Mount a fresh /proc so tools like 'ps' inside the container work
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("[Container] proc mount failed");
        return 1;
    }

    // TODO for Task 3: Redirect stdout/stderr for logging

    // Redirect stdout and stderr to the pipe provided by the supervisor
    if (config->log_write_fd > 0) {
        dup2(config->log_write_fd, STDOUT_FILENO);
        dup2(config->log_write_fd, STDERR_FILENO);
        close(config->log_write_fd);
    }
    printf("[Container %s] Starting command: %s\n", config->id, config->command);
    
    // 4. Apply the scheduler priority (nice value)
    if (config->nice_value != 0) {
        nice(config->nice_value);
    }
    
    // 3. Execute the requested command using /bin/sh
    char *exec_args[] = {"/bin/sh", "-c", config->command, NULL};
    execvp(exec_args[0], exec_args);

    // If execvp returns, it failed
    perror("[Container] execvp failed");
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
 
 // Catches SIGCHLD so the Supervisor's accept() loop wakes up to reap zombies
static void sigchld_handler(int sig) {
    (void)sig; 
}
 
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /*
     * TODO:
     *   1) open /dev/container_monitor
     *   2) create the control socket / FIFO / shared-memory channel
     *   3) install SIGCHLD / SIGINT / SIGTERM handling
     *   4) spawn the logger thread
     *   5) enter the supervisor event loop
     */
    // 1) Open the kernel monitor device
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        perror("Warning: Failed to open /dev/container_monitor");
        // We won't exit here so we can test user-space stuff first
    }

    // 2) Create the control socket
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket setup failed");
        goto cleanup;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    unlink(CONTROL_PATH); // Clean up any old socket left behind
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        goto cleanup;
    }
    
    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen failed");
        goto cleanup;
    }
    
    // 3) Install SIGCHLD handling to wake up the loop when a container dies
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigaction(SIGCHLD, &sa, NULL);
    
    printf("Supervisor is running and listening on %s\n", CONTROL_PATH);
    
    // 4) Spawn the logger thread
    pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    
    // 5) Enter the supervisor event loop
while (!ctx.should_stop) {

        // --- ZOMBIE REAPING BLOCK ---
        int status;
        pid_t dead_pid;
        // WNOHANG means "check for dead children, but don't pause if they are all alive"
        while ((dead_pid = waitpid(-1, &status, WNOHANG)) > 0) {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *curr = ctx.containers;
            while (curr) {
                if (curr->host_pid == dead_pid) {
                    // Follow the rubric's exact rules for classifying the death
                    if (curr->stop_requested) {
                        curr->state = CONTAINER_STOPPED;
                    } else if (WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) {
                        curr->state = CONTAINER_KILLED;
                    } else {
                        curr->state = CONTAINER_EXITED;
                    }
                    curr->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 0;
                    curr->exit_signal = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
                    printf("[Supervisor] Reaped container %s. New state: %s\n", curr->id, state_to_string(curr->state));
                    if (ctx.monitor_fd >= 0) {
                        unregister_from_monitor(ctx.monitor_fd, curr->id, curr->host_pid);
                    }
                    break;
                }
                curr = curr->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
        }
        // ----------------------------

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue; // Loop woke up because a child died, go reap it!
            continue;
        }

        control_request_t req;
        control_response_t res;
        memset(&res, 0, sizeof(res));

        if (read(client_fd, &req, sizeof(req)) == sizeof(req)) {
            
            if (req.kind == CMD_START) {
                printf("[Supervisor] Launching container: %s\n", req.container_id);

                // 1. Allocate a stack for the new child process
                void *child_stack = malloc(STACK_SIZE);
                if (!child_stack) {
                    res.status = 1;
                    strncpy(res.message, "Failed to allocate memory for container stack", sizeof(res.message) - 1);
                } else {
                    // 2. Prepare the config for the child
                    child_config_t *config = malloc(sizeof(child_config_t));
                    strncpy(config->id, req.container_id, sizeof(config->id) - 1);
                    strncpy(config->rootfs, req.rootfs, sizeof(config->rootfs) - 1);
                    strncpy(config->command, req.command, sizeof(config->command) - 1);
                    
                    int log_pipe[2];
                    if (pipe(log_pipe) != 0) {
                        perror("pipe failed");
                    }
                    config->log_write_fd = log_pipe[1]; // Give write end to container
                    
                    // 3. The magic clone() call to create isolated namespaces
                    pid_t child_pid = clone(child_fn, child_stack + STACK_SIZE, 
                                          CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD, 
                                          config);

                    if (child_pid == -1) {
                        res.status = 1;
                        strncpy(res.message, "clone() failed - did you run as sudo?", sizeof(res.message) - 1);
                    } else {
                        res.status = 0;
                        snprintf(res.message, sizeof(res.message), "Container %s started with Host PID: %d", req.container_id, child_pid);
                        
                        close(log_pipe[1]); // Supervisor main thread doesn't write

                        // Start the producer thread to listen to this container
                        producer_ctx_t *pctx = malloc(sizeof(producer_ctx_t));
                        pctx->read_fd = log_pipe[0]; // Give read end to producer
                        snprintf(pctx->container_id, sizeof(pctx->container_id), "%s", req.container_id);
                        pctx->buffer = &ctx.log_buffer;
                        
                        pthread_t prod_tid;
                        pthread_create(&prod_tid, NULL, producer_thread, pctx);
                        pthread_detach(prod_tid); // Let it clean itself up later
                      // Save metadata to ctx.containers linked list
                        container_record_t *rec = malloc(sizeof(container_record_t));
                        memset(rec, 0, sizeof(*rec));
                        strncpy(rec->id, req.container_id, sizeof(rec->id) - 1);
                        rec->host_pid = child_pid;
                        rec->started_at = time(NULL);
                        rec->state = CONTAINER_RUNNING;
                        rec->soft_limit_bytes = req.soft_limit_bytes;
                        rec->hard_limit_bytes = req.hard_limit_bytes;
                        
                        // Thread-safe insert at the head of the list
                        pthread_mutex_lock(&ctx.metadata_lock);
                        rec->next = ctx.containers;
                        ctx.containers = rec;
                        pthread_mutex_unlock(&ctx.metadata_lock);
                        // Tell the kernel module to start watching this PID
                        if (ctx.monitor_fd >= 0) {
                            register_with_monitor(ctx.monitor_fd, req.container_id, child_pid, req.soft_limit_bytes, req.hard_limit_bytes);
                        }
                    }
                }
            } else if (req.kind == CMD_PS) {
                // Loop through the linked list and build a string of all containers
                pthread_mutex_lock(&ctx.metadata_lock);
                container_record_t *curr = ctx.containers;
                char buffer[CONTROL_MESSAGE_LEN] = "";
                
                if (!curr) {
                    strncpy(buffer, "No containers tracked.", sizeof(buffer) - 1);
                } else {
                    while (curr) {
                        char line[64];
                        snprintf(line, sizeof(line), "\nID: %s | PID: %d | State: %s", 
                                 curr->id, curr->host_pid, state_to_string(curr->state));
                        strncat(buffer, line, sizeof(buffer) - strlen(buffer) - 1);
                        curr = curr->next;
                    }
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
                
                res.status = 0;
                strncpy(res.message, buffer, sizeof(res.message) - 1);
                printf("[Supervisor] Sent PS list to client.\n");
                
            } else if (req.kind == CMD_LOGS) {
                char filepath[PATH_MAX];
                snprintf(filepath, sizeof(filepath), "%s/%s.log", LOG_DIR, req.container_id);
                int fd = open(filepath, O_RDONLY);
                if (fd < 0) {
                    res.status = 1;
                    snprintf(res.message, sizeof(res.message), "No log file found for %s", req.container_id);
                } else {
                    char tail[200] = {0};
                    if (read(fd, tail, 199) >= 0) {
                        res.status = 0;
                        snprintf(res.message, sizeof(res.message), "\n--- LOG PREVIEW ---\n%s\n-------------------", tail);
                    }
                    close(fd);
                }
                
            } else if (req.kind == CMD_STOP) {
                pthread_mutex_lock(&ctx.metadata_lock);
                container_record_t *curr = ctx.containers;
                int found = 0;
                
                while (curr) {
                    if (strncmp(curr->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                        found = 1;
                        if (curr->state == CONTAINER_RUNNING) {
                            curr->stop_requested = 1;      // Rule: Set flag BEFORE signaling
                            kill(curr->host_pid, SIGTERM); // Ask process to shut down politely
                            res.status = 0;
                            snprintf(res.message, sizeof(res.message), "Sent stop signal to container %s", curr->id);
                        } else {
                            res.status = 1;
                            snprintf(res.message, sizeof(res.message), "Container %s is already stopped/exited", curr->id);
                        }
                        break;
                    }
                    curr = curr->next;
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
                
                if (!found) {
                    res.status = 1;
                    snprintf(res.message, sizeof(res.message), "Container %s not found", req.container_id);
                }

            } else {
                res.status = 0;
                snprintf(res.message, sizeof(res.message), "Command logic is not implemented yet.");
            }

            // Send response back to CLI
            write(client_fd, &res, sizeof(res));
        }
        close(client_fd);
    }

cleanup:
    if (ctx.server_fd >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 1;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    // Try to connect to the Supervisor
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is the supervisor running?)");
        close(sock);
        return 1;
    }

    // Send the request
    if (write(sock, req, sizeof(*req)) != sizeof(*req)) {
        perror("write");
        close(sock);
        return 1;
    }

    // Wait for the Supervisor to reply
    control_response_t res;
    if (read(sock, &res, sizeof(res)) > 0) {
        printf("Supervisor replied: %s\n", res.message);
    }

    close(sock);
    return res.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
