package raft

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type ClientEnd struct {
	addr string
}

func (clientEnd *ClientEnd) Call(serviceMethod string, args interface{}, reply interface{}) bool {
	var client *rpc.Client
	var err error

	if client, err = rpc.DialHTTP("tcp", clientEnd.addr); err != nil {
		return false
	}
	defer client.Close()

	if err = client.Call(serviceMethod, args, reply); err != nil {
		return false
	}
	return true
}

func (rf *Raft) initRpcPeers(addrs []string) {
	peers := make([]*ClientEnd, 0)
	for _, addr := range addrs {
		peers = append(peers, &ClientEnd{addr: addr})
	}
	rf.peers = peers
	return
}

func (rf *Raft) initRpcServer() {
	server := rpc.NewServer()
	server.Register(rf)

	var err error
	var listener net.Listener
	if listener, err = net.Listen("tcp", rf.peers[rf.me].addr); err != nil {
		log.Fatal(err)
	}
	if err = http.Serve(listener, server); err != nil {
		log.Fatal(err)
	}
	return
}
