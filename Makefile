.PHONY: generate install-tools

generate:
	@./generate.sh

install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go
