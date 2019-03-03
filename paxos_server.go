package main

import (
	"Paxos"
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

var path, _ = os.Getwd()

func main() {
	//Read Configuration File
	cfg := readConfig()
	var err error
	cfg.SID, err = strconv.Atoi(os.Getenv("PAXOS_SID"))
	if err != nil {
		fmt.Printf("WARNING : PAXOS_SID Enviroment Variable Not found, using default SID in config file\n")
	}
	str, _ := json.MarshalIndent(cfg, "", "   ")

	fmt.Printf("[*] Starting Paxos Server With The following Configuration\n%s\n", str)
	//init Proposer,Acceptor, LeaderElector, FailureDetector, ThisServer, Learner
	proposer := Paxos.NewProposer(cfg.SID, nil)
	acceptor := Paxos.NewAcceptor(nil)
	learner := Paxos.NewLearner(nil, cfg.SID)
	currentLeader := cfg.GroupInfo.GroupMembers[cfg.GroupInfo.CurrentLeader]
	leaderElector := Paxos.NewLeaderElector(currentLeader, cfg.GroupInfo.CurrentLeader,
		time.Duration(cfg.PollLeaderFreq), time.Duration(cfg.RegainLeadFeq), nil)
	catchUp := Paxos.NewCatchup(nil)

	//The Paxos Server which composed of the above objects
	thisServer := Paxos.NewServer(cfg.SID, learner, acceptor, proposer, leaderElector, catchUp, &cfg.GroupInfo)

	//Set their Server
	proposer.ThisServer = thisServer
	acceptor.ThisServer = thisServer
	learner.ThisServer = thisServer
	leaderElector.ThisServer = thisServer
	catchUp.ThisServer = thisServer
	//Register These Objects for The RPC server so that their methods are reachable
	listener, rpcs := registerRPC(learner, acceptor, proposer, leaderElector, catchUp, &cfg.GroupInfo)
	defer (*listener).Close()
	//Start Polling Leader
	go func() {
		<-time.After(2 * time.Second)
		go leaderElector.PollLeader()
		go leaderElector.Stabilize()
	}()
	//Accept RPC requests
	for {
		conn, err := (*listener).Accept()
		if err != nil {
			fmt.Printf("Error : Listner Error : %s\n", err)
			continue
		}
		go func(conn net.Conn) {
			(*rpcs).ServeConn(conn)
			conn.Close()
		}(conn)
	}
}

func readConfig() (cfg *Configuration) {
	cfg = new(Configuration)
	config, err := os.Open("./paxos.config.json")
	if err != nil {
		fmt.Printf("Error : Unable To Read Config File : %s\n", err)
		os.Exit(-1)
	}
	deco := json.NewDecoder(config)
	deco.Decode(cfg)
	return
}

func registerRPC(lrnr *Paxos.Learner, acc *Paxos.Acceptor, pr *Paxos.Proposer,
	ldr *Paxos.LeaderElector, cu *Paxos.CatchUp, group *Paxos.GroupInfo) (*net.Listener, *rpc.Server) {

	//Create A new RPC server and Listner
	rpcs := rpc.NewServer()
	servAddr := group.GroupMembers[pr.SID]
	l, err := net.Listen("tcp4", servAddr)
	if err != nil {
		fmt.Printf("Error : RegisterRPC : %s\n", err)
		panic("Couldn't Register the Objects\n")
	}
	rpcs.Register(lrnr)
	rpcs.Register(acc)
	rpcs.Register(pr)
	rpcs.Register(ldr)
	rpcs.Register(cu)

	return &l, rpcs
}

type Configuration struct {
	SID            int
	GroupInfo      Paxos.GroupInfo
	PollLeaderFreq time.Duration
	RegainLeadFeq  time.Duration
}
