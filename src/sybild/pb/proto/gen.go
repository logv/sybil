//go:generate protoc -I . -I ../../../../../.. sybil.proto --go_out=plugins=grpc:../

package proto
