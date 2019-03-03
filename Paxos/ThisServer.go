package Paxos

import "sync"

type ThisServer struct {
	sync.Mutex
	SID            int
	ServerAddress  string
	tLearner       *Learner
	tProposer      *Proposer
	tAcceptor      *Acceptor
	tLeaderElector *LeaderElector
	tCu            *CatchUp
	GroupInfoPtr   *GroupInfo
	OpSeq          int                  //Last seen operation
	Clients        map[Operation]string //to send response to the client
}

func NewServer(SID int, lrnr *Learner, acc *Acceptor, pr *Proposer,
	ldr *LeaderElector, cu *CatchUp, group *GroupInfo) (serv *ThisServer) {

	serv = new(ThisServer)
	serv.SID = SID
	serv.ServerAddress = group.GroupMembers[SID]
	serv.tAcceptor = acc
	serv.tLearner = lrnr
	serv.tProposer = pr
	serv.tLeaderElector = ldr
	serv.tCu = cu
	serv.GroupInfoPtr = group
	serv.OpSeq = 1
	serv.Clients = make(map[Operation]string)
	return
}
