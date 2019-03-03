package main

import (
	"Paxos"
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

var cfg *Configuration

func main() {
	cfg = readConfig()
	// cfg.CurrentLeader =
	i, e := strconv.Atoi(os.Getenv("PAXOS_SID"))
	if e != nil {
		fmt.Printf("WARNING PAXOS_SID NOT SET")
		i = cfg.CurrentLeader
	}
	cfg.CurrentLeader = i
	data, _ := json.MarshalIndent(cfg, "", "   ")
	fmt.Printf("Paxos Client Running with the following Configuration\n%s\n", data)
	addr := startRPC()
	var val int
	for {

		fmt.Printf("Value To Be Submitted : ")
		_, err := fmt.Scanf("%d", &val)
		if err != nil {
			continue
		}
		if val == -1 {
			fmt.Printf("Exiting....")
			os.Exit(-1)
		}
		res := submitOperation(val, addr)
		handleResponse(val, res, addr)
		fmt.Printf("\nWaiting For response....\n")
	}
}

type Client struct {
}

func (c *Client) RequestDone(op *Paxos.Operation, _ *interface{}) error {
	str, _ := json.Marshal(op)
	fmt.Printf("\n\n[*] Info : Operation %s Has been Executed/Choosen\n\n", str)
	fmt.Printf("Value To Be Submitted : ")
	return nil
}

func submitOperation(val int, addr string) (res *Paxos.OperationExRes) {
	//Create args and reply
	request := new(Paxos.OperationExReq)
	request.ClientAddress = addr
	request.Operation.Value = val
	res = new(Paxos.OperationExRes)
	//Contact Paxos leader / server/
	currentLeader := cfg.PaxosGroup[cfg.CurrentLeader]
	client, err := rpc.Dial("tcp4", currentLeader)
	if err != nil {
		fmt.Printf("\nError : Cannot Connect to Leader/Paxos Server/ : %s \n", currentLeader)
		res.Succeded = false
		return
	}
	err = client.Call("Proposer.NewOperation", &request, res)
	if err != nil {
		fmt.Printf("\nError: Couldnt Submit Operation\n")
		res.Succeded = false
		return
	}
	return
}

func handleResponse(val int, res *Paxos.OperationExRes, addr string) {
	if res.Succeded {
		fmt.Printf("\nRequest Submitted Successfully! Waiting for Completion Message\n")
		return
	}
	for !res.Succeded && !res.Redirect && cfg.CurrentLeader < len(cfg.PaxosGroup) {
		//Failed Leader, Try other servers
		cfg.CurrentLeader++
		res = submitOperation(val, addr)
	}
	if cfg.CurrentLeader == len(cfg.PaxosGroup) {
		cfg.CurrentLeader = 0
		fmt.Printf("\nRequest Submission Failed Couldn't reach any server")
		return
	}
	for res.Redirect {
		fmt.Printf("\nLeader Might Have Changed, Redirecting To A New Leader...\n")
		i := findIndexOfServer(res.LeaderAddr)
		if i == -1 {
			fmt.Printf("\nRequest Submission Failed Couldn't reach New Leader\n")
			cfg.CurrentLeader = 0
			return
		}
		cfg.CurrentLeader = i
		res = submitOperation(val, addr)
	}

}
func findIndexOfServer(serv string) int {

	for i, s := range cfg.PaxosGroup {
		if s == serv {
			return i
		}
	}
	return -1
}
func startRPC() string {
	client := new(Client)
	rpcs := rpc.NewServer()
	l, err := net.Listen("tcp4", "127.0.0.1:")
	if err != nil {
		fmt.Printf("\nError Starting Client \n%s\n", err)
		os.Exit(-1)
	}

	rpcs.Register(client)
	//Wait for response
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Printf("\nProblem Accepting RPC\n")
				continue
			}
			go rpcs.ServeConn(conn)
		}
	}()
	return l.Addr().String()
}

func readConfig() (cfg *Configuration) {
	cfg = new(Configuration)
	config, err := os.Open("./client.config.json")
	if err != nil {
		fmt.Printf("Error : Unable To Read Config File : %s\n", err)
		os.Exit(-1)
	}
	deco := json.NewDecoder(config)
	deco.Decode(cfg)
	return
}

type Configuration struct {
	ClientAddress string
	CurrentLeader int
	PaxosGroup    []string
}
