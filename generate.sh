#!/bin/bash
rm -rf pb/* && cd proto && protoc -I. --gofast_out=paths=source_relative:../pb shop.proto
