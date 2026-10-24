# Distributed Systems Labs: Coordination and Broadcast

This directory contains two Go-based distributed-systems exercises. Each process runs as a separate operating-system process and communicates with its peers over local TCP sockets. The C wrappers in the Go programs make the socket calls visible to the supplied `LD_PRELOAD` hook.

## Contents

- [`Lab01/`](Lab01/README.md): a distributed mutual-exclusion lock ordered by Lamport timestamps and process IDs.
- [`Lab02/`](Lab02/README.md): simplified Raft-style and Bracha reliable-broadcast protocols.
- [`results.txt`](results.txt): a recorded test-results snapshot. Its test names and totals do not match the test files currently present, so treat it as historical rather than as a current test report.

Both labs require Linux, Go with cgo enabled, a C compiler, Perl, and the standard C dynamic-loader library. Run the commands below from the corresponding lab directory.

## Lab01: Distributed Lock

Lab01 implements a fair distributed lock. Processes broadcast timestamped lock requests, maintain a total order by `(Lamport timestamp, process ID)`, acknowledge requests, and announce releases. A process enters its critical section when its request is first in that order and it has received the required acknowledgements.

`critical.c` records lock-entry and release events in `log.txt`. `hook.c` supplies the socket tracing shim. The three fixtures in `Lab01/tests/` exercise competing lock requests and explicit wait dependencies.

```sh
cd Lab01
make             # Build critical, hook.so, and process
make check       # Build and run all tests
make log_reset   # Clear log.txt
make clean       # Remove generated executables and shared libraries
```

To run one fixture after building:

```sh
./run.pl ./tests/test01
```

The runner checks the output format, timestamp ordering, balanced lock/release events, lock counts, and declared wait constraints. Acquisition order can vary when the input permits more than one valid order.

## Lab02: Reliable Broadcast

Lab02 provides two protocol modes, selected with `PROTOCOL`:

- `RAFT`: process 0 proposes to the configured peers, collects acknowledgements, delivers after its acknowledgement threshold, and sends commits.
- `BRACHA`: processes exchange ECHO and VOTE messages and count distinct senders per value against the Bracha thresholds, with $f = \lfloor (N-1)/3 \rfloor$.

The input file starts with the process count and then contains broadcast commands:

```text
N
BCAST value recipient_count
```

For example, `BCAST 42 3` starts a broadcast of value `42` to the first three peer IDs after process 0 (subject to the process-count limit). Values are encoded as 32-bit unsigned fields on the wire, so use integers in the range `0` through `4294967295` for the fixtures supported by this implementation.

```sh
cd Lab02
make             # Build process
make check_raft  # Run every fixture in RAFT mode
make check_bracha # Run every fixture in BRACHA mode
make clean       # Remove the generated process executable
```

The test runner builds the Lab01 socket hook as a prerequisite. To run an individual fixture:

```sh
make
make -C ../Lab01 hook.so
env PROTOCOL=BRACHA ./run.pl ./tests/test01
```
