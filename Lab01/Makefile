.PHONY: all clean log_reset check

all: critical hook.so process

critical: critical.c
	$(CC) critical.c -o critical

hook.so: hook.c
	$(CC) -shared -fPIC -o hook.so hook.c -ldl

process: process.go
	CGO_ENABLED=1 go build -o process process.go

check:
	$(MAKE) clean
	$(MAKE) all
	for f in $(wildcard ./tests/*); do \
		echo "=== Running $$f ==="; \
		./run.pl "$$f"; \
	done

clean:
	rm -f critical hook.so process

log_reset:
	rm -f log.txt
