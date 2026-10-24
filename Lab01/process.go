package main

/*
#cgo LDFLAGS: -ldl
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

// C wrappers to strictly trigger LD_PRELOAD interceptors
int c_socket() {
    return socket(AF_INET, SOCK_STREAM, 0);
}

int c_bind(int fd, int port) {
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    return bind(fd, (struct sockaddr *)&addr, sizeof(addr));
}

int c_listen(int fd, int backlog) {
    return listen(fd, backlog);
}

int c_accept(int fd) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int ret;
    do {
        ret = accept(fd, (struct sockaddr *)&client_addr, &client_len);
    } while (ret == -1 && errno == EINTR);
    return ret;
}

int c_connect(int fd, const char* ip, int port) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);
    int ret;
    do {
        ret = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    } while (ret == -1 && errno == EINTR);
    return ret;
}

ssize_t c_send(int fd, const void *buf, size_t len) {
    ssize_t ret;
    do {
        ret = send(fd, buf, len, 0);
    } while (ret == -1 && errno == EINTR);
    return ret;
}

ssize_t c_recv(int fd, void *buf, size_t len) {
    ssize_t ret;
    do {
        ret = recv(fd, buf, len, 0);
    } while (ret == -1 && errno == EINTR);
    return ret;
}

void c_close(int fd) {
    close(fd);
}
*/
import "C"

import (
    "bufio"
    "encoding/binary"
    "fmt"
    "os"
    "os/exec"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
    "unsafe"
)

const (
    REQ  = 0
    ACK  = 1
    REL  = 2
    INFO = 3 // Sent upon connection to establish caller's ID
)

type Request struct {
    LC  int
    PID int
}

type Message struct {
    Type int
    LC   int
    PID  int
}

type Command struct {
    Type string
    Arg  int
}

var (
    mu                  sync.Mutex
    cond                = sync.NewCond(&mu)
    clock               int
    queue               []Request
    has_ack             []bool
    releases            []int
    total_releases_seen int
    total_locks_global  int
    myReqLC             int
    waited_for          []int
    peers               []int
    myID                int
    N                   int
)

func maxInt(a, b int) int {
    if a > b {
        return a
    }
    return b
}

func sortQueue(q []Request) {
    sort.SliceStable(q, func(i, j int) bool {
        if q[i].LC == q[j].LC {
            return q[i].PID < q[j].PID
        }
        return q[i].LC < q[j].LC
    })
}

func removeQueue(pid int) {
    for i, req := range queue {
        if req.PID == pid {
            queue = append(queue[:i], queue[i+1:]...)
            return
        }
    }
}

func sendMsg(fd int, msgType, lc, pid int) {
    buf := make([]byte, 12)
    binary.LittleEndian.PutUint32(buf[0:4], uint32(msgType))
    binary.LittleEndian.PutUint32(buf[4:8], uint32(lc))
    binary.LittleEndian.PutUint32(buf[8:12], uint32(pid))

	// mechanism to make sure we sent all n
    sent := 0
    for sent < 12 {
        n := int(C.c_send(C.int(fd), unsafe.Pointer(&buf[sent]), C.size_t(12-sent)))
        if n <= 0 {
            panic(fmt.Sprintf("Failed to send data to fd %d", fd))
        }
        sent += n
    }
}

func recvMsg(fd int) Message {
    buf := make([]byte, 12)
	// mechnaism to make sure we received all of n
    received := 0
    for received < 12 {
        n := int(C.c_recv(C.int(fd), unsafe.Pointer(&buf[received]), C.size_t(12-received)))
        if n <= 0 {
            return Message{Type: -1}
        }
        received += n
    }
    return Message{
        Type: int(binary.LittleEndian.Uint32(buf[0:4])),
        LC:   int(binary.LittleEndian.Uint32(buf[4:8])),
        PID:  int(binary.LittleEndian.Uint32(buf[8:12])),
    }
}

func broadcast(msgType, lc, pid int) {
    for p, fd := range peers {
        if p != myID {
            sendMsg(fd, msgType, lc, pid)
        }
    }
}

func handleMsg(msg Message, peer int) {
    mu.Lock()
    if msg.Type == INFO {
        mu.Unlock()
        return
    }

	// Algorithm execution
    // 1. Advance logical clock on receive
    clock = maxInt(clock, msg.LC) + 1

    switch msg.Type {
    case REQ:
        queue = append(queue, Request{msg.LC, msg.PID})
        sortQueue(queue)

        // 2. Strict Lamport clock: Increment before sending ACK
        clock++
        ackLC := clock
        mu.Unlock()

        // Reply immediately without blocking mutex
        sendMsg(peers[peer], ACK, ackLC, myID)

        mu.Lock()
        cond.Broadcast()
        mu.Unlock()
        return

    case ACK:
        if myReqLC > 0 && msg.LC > myReqLC {
            has_ack[msg.PID] = true
        }

    case REL:
        removeQueue(msg.PID)
        releases[msg.PID]++
        total_releases_seen++
    }
    cond.Broadcast()
    mu.Unlock()
}

func parseFile(filename string, myID int) ([]Command, int) {
    file, err := os.Open(filename)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)

    scanner.Scan()
    firstLine := (scanner.Text())
    N, _ = strconv.Atoi(strings.TrimSpace(firstLine))

    var myCmds []Command
    totalLocks := 0

    for scanner.Scan() {
        text := strings.TrimSpace((scanner.Text()))
        if text == "" {
            continue
        }
        parts := strings.Fields(text)
        pid, _ := strconv.Atoi(parts[0])

        switch parts[1] {
        case "Lock":
            totalLocks++
			//TODO refactor
            if pid == myID {
                dur, _ := strconv.Atoi(parts[2])
                myCmds = append(myCmds, Command{Type: "Lock", Arg: dur})
            }
        case "Wait":
			//TODO refactor
            if pid == myID {
                target, _ := strconv.Atoi(parts[2])
                myCmds = append(myCmds, Command{Type: "Wait", Arg: target})
            }
        }
    }
    return myCmds, totalLocks
}

func main() {

    myID, _ = strconv.Atoi(os.Args[1])
    myCmds, totalLocksGlobal := parseFile(os.Args[2], myID)
    total_locks_global = totalLocksGlobal

    has_ack = make([]bool, N)
    releases = make([]int, N)
    waited_for = make([]int, N)
    peers = make([]int, N)

    basePort := 9000
    listenFd := C.c_socket()
    C.c_bind(C.int(listenFd), C.int(basePort+myID))
    C.c_listen(C.int(listenFd), C.int(N))

    ip := C.CString("127.0.0.1")
    defer C.free(unsafe.Pointer(ip))

    // Lower IDs act as servers, higher IDs act as clients iteratively (Avoids Deadlocks)
    for i := 0; i < myID; i++ {
        for {
            fd := C.c_socket()
            res := C.c_connect(C.int(fd), ip, C.int(basePort+i))
            if res == 0 {
                peers[i] = int(fd)
                sendMsg(int(fd), INFO, 0, myID)
                break
            }
            C.c_close(C.int(fd))
            time.Sleep(50 * time.Millisecond)
        }
    }

    // Accept connections from higher IDs
    for i := myID + 1; i < N; i++ {
        fd := C.c_accept(C.int(listenFd))
        msg := recvMsg(int(fd))
        if msg.Type == INFO {
            peers[msg.PID] = int(fd)
        }
    }

    for p := 0; p < N; p++ {
        if p == myID {
            continue
        }
        go func(peer, fd int) {
            for {
                msg := recvMsg(fd)
                if msg.Type == -1 {
                    break
                }
                handleMsg(msg, peer)
            }
        }(p, peers[p])
    }

    // Execute Commands sequentially
    for _, cmd := range myCmds {
        if cmd.Type == "Wait" {
            targetID := cmd.Arg
            waited_for[targetID]++
            targetCount := waited_for[targetID]

            mu.Lock()
            for releases[targetID] < targetCount {
                cond.Wait()
            }
            mu.Unlock()

        } else if cmd.Type == "Lock" {
            dur := cmd.Arg

            mu.Lock()
            clock++
            myReqLC = clock
            for i := 0; i < N; i++ {
                has_ack[i] = false
            }
            queue = append(queue, Request{myReqLC, myID})
            sortQueue(queue)
            reqLCCopy := myReqLC
            mu.Unlock()

            broadcast(REQ, reqLCCopy, myID)

            mu.Lock()
            for {
                if len(queue) > 0 && queue[0].PID == myID && queue[0].LC == myReqLC {
                    allAcked := true
                    for i := 0; i < N; i++ {
                        if i != myID && !has_ack[i] {
                            allAcked = false
                            break
                        }
                    }
                    if allAcked {
                        break
                    }
                }
                cond.Wait()
            }
            mu.Unlock()

            // CRITICAL SECTION
            execCmd := exec.Command("./critical", strconv.Itoa(myID), strconv.Itoa(dur))
            execCmd.Stdout = os.Stdout
            execCmd.Stderr = os.Stderr
            execCmd.Run()

            mu.Lock()
            removeQueue(myID)
            clock++
            relLC := clock
            myReqLC = 0
            releases[myID]++
            total_releases_seen++
            mu.Unlock()

            broadcast(REL, relLC, myID)

            mu.Lock()
            cond.Broadcast()
            mu.Unlock()
        }
    }

    // Block main until ALL nodes finish processing ALL locks globally
    mu.Lock()
    for total_releases_seen < total_locks_global {
        cond.Wait()
    }
    mu.Unlock()

    for _, fd := range peers {
        if fd != 0 {
            C.c_close(C.int(fd))
        }
    }
    C.c_close(C.int(listenFd))
}