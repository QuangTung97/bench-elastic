#!/bin/bash
rm -rf pb && cd proto && protoc -I. --go_out=paths=source_relative:../pb shop.proto
