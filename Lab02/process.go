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
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	INFO    = 0
	READY   = 1
	PROPOSE = 2

	// Raft specific message types
	ACK    = 3
	COMMIT = 4

	// Bracha specific message types
	ECHO = 3
	VOTE = 4
)

type Message struct {
	Type   int
	Val    int
	Sender int
}

type BcastCmd struct {
	Val   int
	Count int
}

var (
	mu         sync.Mutex
	cond       = sync.NewCond(&mu)
	peers      []int
	peerLock   []sync.Mutex
	myID       int
	N          int
	readyCount int

	// Raft state variables
	ackCount map[int]int // Count the number of ACKs for each value received

	// Bracha state variables
	echoSenders map[int]map[int]bool
	voteSenders map[int]map[int]bool
	sentEcho    map[int]bool
	sentVote1   map[int]bool
	sentVote2   map[int]bool

	// Shared delivery state
	deliveredVal map[int]bool
)

func isBracha() bool {
	for _, env := range os.Environ() {
		pair := strings.SplitN(env, "=", 2)
		if len(pair) == 2 {
			val := strings.ToLower(strings.TrimSpace(pair[1]))
			if strings.Contains(val, "bracha") || val == "2" {
				return true
			}
			if strings.Contains(val, "raft") || val == "1" {
				return false
			}
		}
	}
	return false
}

func sendMsg(peer int, msgType, val, sender int) {
	peerLock[peer].Lock()
	defer peerLock[peer].Unlock()

	fd := peers[peer]
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(msgType))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(val))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(sender))

	sent := 0
	for sent < 12 {
		n := int(C.c_send(C.int(fd), unsafe.Pointer(&buf[sent]), C.size_t(12-sent)))
		if n <= 0 {
			panic(fmt.Sprintf("Failed to send data to fd %d (peer %d)", fd, peer))
		}
		sent += n
	}
}

func recvMsg(fd int) Message {
	buf := make([]byte, 12)
	received := 0
	for received < 12 {
		n := int(C.c_recv(C.int(fd), unsafe.Pointer(&buf[received]), C.size_t(12-received)))
		if n <= 0 {
			return Message{Type: -1}
		}
		received += n
	}
	return Message{
		Type:   int(binary.LittleEndian.Uint32(buf[0:4])),
		Val:    int(binary.LittleEndian.Uint32(buf[4:8])),
		Sender: int(binary.LittleEndian.Uint32(buf[8:12])),
	}
}

func broadcastAllBracha(msgType, val int) {
	mu.Lock()
	if msgType == ECHO {
		if echoSenders[val] == nil {
			echoSenders[val] = make(map[int]bool)
		}
		echoSenders[val][myID] = true
	} else if msgType == VOTE {
		if voteSenders[val] == nil {
			voteSenders[val] = make(map[int]bool)
		}
		voteSenders[val][myID] = true
	}
	checkThresholdsBracha(val)
	mu.Unlock()

	for p := 0; p < N; p++ {
		if p != myID && peers[p] != 0 {
			sendMsg(p, msgType, val, myID)
		}
	}
}

func checkThresholdsBracha(val int) {
	f := (N - 1) / 3
	nMinusF := N - f
	fPlus1 := f + 1

	if echoSenders[val] != nil && len(echoSenders[val]) >= nMinusF && !sentVote1[val] {
		sentVote1[val] = true
		sentVote2[val] = true
		go broadcastAllBracha(VOTE, val)
	}

	if voteSenders[val] != nil && len(voteSenders[val]) >= fPlus1 && !sentVote2[val] {
		sentVote2[val] = true
		go broadcastAllBracha(VOTE, val)
	}

	if voteSenders[val] != nil && len(voteSenders[val]) >= nMinusF && !deliveredVal[val] {
		deliveredVal[val] = true
		fmt.Printf("[Process %d] DELIVERED %d\n", myID, val)
	}
}

func handleMsg(msg Message) {
	mu.Lock()
	defer mu.Unlock()

	switch msg.Type {
	case INFO:
		return
	case READY:
		readyCount++
		cond.Broadcast()
	case PROPOSE:
		if isBracha() {
			if !sentEcho[msg.Val] {
				sentEcho[msg.Val] = true
				go broadcastAllBracha(ECHO, msg.Val)
			}
		} else {
			// 2. Receive Propose: Send [Ack] to the leader
			sendMsg(msg.Sender, ACK, msg.Val, myID)
		}
	case 3: // ACK (Raft) or ECHO (Bracha)
		if isBracha() {
			if echoSenders[msg.Val] == nil {
				echoSenders[msg.Val] = make(map[int]bool)
			}
			echoSenders[msg.Val][msg.Sender] = true
			checkThresholdsBracha(msg.Val)
			cond.Broadcast()
		} else {
			// 3. Receive ACK: Pre treatement increment the ackCount
			ackCount[msg.Val]++
			cond.Broadcast()
		}
	case 4: // COMMIT (Raft) or VOTE (Bracha)
		if isBracha() {
			if voteSenders[msg.Val] == nil {
				voteSenders[msg.Val] = make(map[int]bool)
			}
			voteSenders[msg.Val][msg.Sender] = true
			checkThresholdsBracha(msg.Val)
			cond.Broadcast()
		} else {
			// 4. Receive Commit: Deliver the message
			if !deliveredVal[msg.Val] {
				deliveredVal[msg.Val] = true
				fmt.Printf("[Process %d] DELIVERED %d\n", myID, msg.Val)
			}
			cond.Broadcast()
		}
	}
}

func parseFile(filename string, myID int) ([]BcastCmd, int) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return nil, 0
	}
	nVal, _ := strconv.Atoi(strings.TrimSpace(scanner.Text()))

	var cmds []BcastCmd
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		parts := strings.Fields(text)
		if len(parts) >= 3 && parts[0] == "BCAST" {
			val, _ := strconv.Atoi(parts[1])
			count, _ := strconv.Atoi(parts[2])
			cmds = append(cmds, BcastCmd{Val: val, Count: count})
		}
	}
	return cmds, nVal
}

func initGlobalState() {
	peers = make([]int, N)
	peerLock = make([]sync.Mutex, N)
	ackCount = make(map[int]int)
	echoSenders = make(map[int]map[int]bool)
	voteSenders = make(map[int]map[int]bool)
	sentEcho = make(map[int]bool)
	sentVote1 = make(map[int]bool)
	sentVote2 = make(map[int]bool)
	deliveredVal = make(map[int]bool)
}

func setupNetwork() int {
	basePort := 9000
	listenFd := int(C.c_socket())
	C.c_bind(C.int(listenFd), C.int(basePort+myID))
	C.c_listen(C.int(listenFd), C.int(N))

	ip := C.CString("127.0.0.1")
	defer C.free(unsafe.Pointer(ip))

	// Lower IDs act as servers, higher IDs connect to them
	for i := 0; i < myID; i++ {
		for {
			fd := int(C.c_socket())
			res := C.c_connect(C.int(fd), ip, C.int(basePort+i))
			if res == 0 {
				peers[i] = fd
				sendMsg(i, INFO, 0, myID)
				break
			}
			C.c_close(C.int(fd))
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Accept connections from higher IDs
	for i := myID + 1; i < N; i++ {
		fd := int(C.c_accept(C.int(listenFd)))
		msg := recvMsg(fd)
		if msg.Type == INFO {
			peers[msg.Sender] = fd
		}
	}

	return listenFd
}

func startMessageHandlers() {
	for p := 0; p < N; p++ {
		if p == myID {
			continue
		}
		go func(fd int) {
			for {
				msg := recvMsg(fd)
				if msg.Type == -1 {
					break
				}
				handleMsg(msg)
			}
		}(peers[p])
	}
}

func teardownNetwork(listenFd int) {
	for _, fd := range peers {
		if fd != 0 {
			C.c_close(C.int(fd))
		}
	}
	C.c_close(C.int(listenFd))
}

func broadcastRaft(msgType, val int, count int) {
	for p := 1; p <= count && p < N; p++ {
		if p != myID {
			sendMsg(p, msgType, val, myID)
		}
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <myID> <config_file>\n", os.Args[0])
		os.Exit(1)
	}

	myID, _ = strconv.Atoi(os.Args[1])
	cmds, nVal := parseFile(os.Args[2], myID)
	N = nVal
	initGlobalState()

	listenFd := setupNetwork()
	defer teardownNetwork(listenFd)

	startMessageHandlers()

	// sync barrier for READY
	for p := 0; p < N; p++ {
		if p != myID {
			sendMsg(p, READY, 0, myID)
		}
	}
	mu.Lock()
	for readyCount < N-1 {
		cond.Wait()
	}
	mu.Unlock()

	if isBracha() {
		// Bracha Execution Flow
		if myID == 0 {
			for _, cmd := range cmds {
				handleMsg(Message{Type: PROPOSE, Val: cmd.Val, Sender: myID})
				for p := 1; p < N; p++ {
					sendMsg(p, PROPOSE, cmd.Val, myID)
				}
				time.Sleep(300 * time.Millisecond)
			}
		}
		time.Sleep(600 * time.Millisecond)
	} else {
		// Raft Execution Flow
		if myID == 0 {
			for _, cmd := range cmds {
				// 1. Propose: The leader sends [Propose, msg] to all followers.
				broadcastRaft(PROPOSE, cmd.Val, cmd.Count)

				// Wiat for majority of ACKs
				requiredAcks := cmd.Count / 2

				mu.Lock()
				for ackCount[cmd.Val] < requiredAcks {
					cond.Wait()
				}
				// 3. Receive ACK : When a majority of followers have acknowledged the message, deliver the message and send [Commit] to all followers.
				if !deliveredVal[cmd.Val] {
					deliveredVal[cmd.Val] = true
					fmt.Printf("[Process %d] DELIVERED %d\n", myID, cmd.Val)
				}
				mu.Unlock()

				broadcastRaft(COMMIT, cmd.Val, cmd.Count)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}
