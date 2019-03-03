package Paxos

import (
	"fmt"
	"net/rpc"
	"sync"
)

const (
	NO_OP     = -1
	NO_LEADER = -2
)

var debugEnabled bool = true
var printLock sync.Mutex

type Proposal struct {
	TS TimeStamp //The Globally unique strictly increasing TS
	Op Operation //The values of the proposal
}

type Promise struct {
	HasPromised    bool
	PrevAcceptedOp Operation
	OpTS           TimeStamp
}
type PrepareMsg struct {
	TS TimeStamp
}

type LearnMsg struct {
	TsedOp      TSedOp
	AcceptorSid int
}
type Operation struct {
	OpID  OperaitonId
	Value int
}

type OperaitonId struct {
	OpSeq int
	SID   int
}
type TSedOp struct {
	Op   Operation
	OpTs TimeStamp
}

type OperationExReq struct {
	Operation     Operation
	ClientAddress string
}
type OperationExRes struct {
	Succeded   bool
	Redirect   bool
	LeaderSID  int
	LeaderAddr string
}

/*Made up of this servers id + a strictly increasing sequence number*/
type TimeStamp struct {
	SID    int
	SeqNum int
}
type GroupInfo struct {
	GroupMembers  map[int]string //A mapping of SID to Server address
	CurrentLeader int            //SID of believed current leader
}
type CatchUpReq struct {
	LastSeenOp struct { //The last update this server has seen
		OpSeq int
		SID   int
	}
}

type CatchUpRes struct {
	LearnedOps []TSedOp //The operations that must be learned
}

type LdrElectionRes struct {
	alive bool
}

/*Just encapsulating the rpc.dial & call code for all to use uniformly*/
func call(serv string, rpcMethod string,
	args interface{}, reply interface{}) bool {

	client, err := rpc.Dial("tcp4", serv)
	if err != nil {
		return false
	}
	defer client.Close()
	err = client.Call(rpcMethod, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}

//Comparng Timestamps

func (ts1 *TimeStamp) Less(ts2 *TimeStamp) (res bool) {
	res = false
	if ts1.SeqNum < ts2.SeqNum {
		res = true
	} else if ts1.SeqNum == ts2.SeqNum && ts1.SID < ts2.SID {
		res = true
	}
	return
}

func debug(format string, a ...interface{}) (n int, err error) {
	printLock.Lock()
	defer printLock.Unlock()
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
		fmt.Println()
	}
	return
}
func debug_err(format string, a ...interface{}) (n int, err error) {
	printLock.Lock()
	defer printLock.Unlock()
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
		fmt.Println()
	}
	return
}

//For sorting Promises
func (pr *Promise) GetTS() TimeStamp {
	return pr.OpTS
}

type ByTSPr []Promise

func (p ByTSPr) Len() int { return len(p) }

func (p ByTSPr) Less(i, j int) bool {
	ts1 := p[i].GetTS()
	ts2 := p[j].GetTS()
	return ts1.SeqNum < ts2.SeqNum || (ts1.SeqNum == ts2.SeqNum && ts1.SID < ts2.SID)
}

func (p ByTSPr) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
