CURR_DIR = $(shell pwd)
BIN_DIR = bin
GO_BUILD = GOPATH=$(CURR_DIR) GOBIN=$(CURR_DIR)/$(BIN_DIR) go install $@

all: server client

server:
	$(GO_BUILD)

client:
	$(GO_BUILD)

.PHONY: clean

clean:
	rm -rf bin pkg
