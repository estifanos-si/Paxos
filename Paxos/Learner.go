package Paxos

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"
)

type Learner struct {
	sync.Mutex

	//Learned operations, This have been Choosen,
	LearnedOps map[int]TSedOp //Mapping of opseq to The specific Operation
	//Until A majority of Acceptors send learn for a specific Operation
	PendingQueue map[OperaitonId]PendingOperation
	ThisServer   *ThisServer
	writer       *json.Encoder
}

func NewLearner(this *ThisServer, sid int) (lr *Learner) {
	lr = new(Learner)
	lr.ThisServer = this
	lr.LearnedOps = make(map[int]TSedOp)
	lr.PendingQueue = make(map[OperaitonId]PendingOperation)
	f, err := os.Create(fmt.Sprintf("learned_ops%d.log", sid))
	if err != nil {
		debug("[*] WARNING : Unable to open log file %s.log", string(lr.ThisServer.SID))
		return
	}
	lr.writer = json.NewEncoder(f)
	return
}

func (lr *Learner) Learn(lrnMsg *LearnMsg, _ *interface{}) error {
	lr.ThisServer.Lock()
	lr.Lock()

	defer lr.Unlock()
	defer lr.ThisServer.Unlock()
	debug("[*] Info : Learner : Learn Message recieved : %v", lrnMsg.AcceptorSid)

	originServ := lr.ThisServer.GroupInfoPtr.GroupMembers[lrnMsg.TsedOp.Op.OpID.SID]
	if !lr.ThisServer.tCu.getOperationsLr(originServ, lrnMsg.TsedOp.Op.OpID.OpSeq) {
		//Not Upto date Can't continue
		debug("[*] Info : Ignoring Learn Msg with OpSeq (%d) != ThisServers.OpSeq (%d)",
			lrnMsg.TsedOp.Op.OpID.OpSeq, lr.ThisServer.OpSeq)
		return nil
	}
	pendingOp, ok := lr.PendingQueue[lrnMsg.TsedOp.Op.OpID]
	if !ok {
		//Haven't seen this operation yet add it to the pending queue
		seenAcceptors := make([]int, 0, 1)
		seenAcceptors = append(seenAcceptors, lrnMsg.AcceptorSid)
		lr.PendingQueue[lrnMsg.TsedOp.Op.OpID] = PendingOperation{lrnMsg.TsedOp, 1, seenAcceptors}
		return nil
	}

	if pendingOp.haveSeenAcceptor(lrnMsg.AcceptorSid) {
		//This acceptor have already sent a learn message for this specific pending Operation
		//This is a retransmission
		return nil
	}

	//Count distinct learn messages for this operation
	pendingOp.count++
	if lr.majority(pendingOp.count) {
		//Have recieved Learn message from majority of acceptors,Learn This Operation
		lr.doLearn(lrnMsg.TsedOp)
		return nil
	}
	pendingOp.seenAcceptors = append(pendingOp.seenAcceptors, lrnMsg.AcceptorSid)
	lr.PendingQueue[lrnMsg.TsedOp.Op.OpID] = pendingOp
	return nil
}

func (lr *Learner) doLearn(tsedOp TSedOp) {
	//Learn this message
	str, _ := json.Marshal(tsedOp)
	debug("[*] Info : Learner : Learning Operation : %s", str)
	lr.catchUp(tsedOp)
	str, _ = json.Marshal(tsedOp)
	debug("%d. Learned Operation %s", lr.ThisServer.OpSeq-1, str)
	//Delete the pendingOperations, in preparation for the next paxos instance
	lr.PendingQueue = make(map[OperaitonId]PendingOperation)
	if lr.ThisServer.tLeaderElector.LeaderSID != lr.ThisServer.SID {
		return
	}
	// This is the leader, Notify The client
	lr.notifyClient(tsedOp)
}

func (lr *Learner) notifyClient(tsedOp TSedOp) {
	debug("[*] Info : Learner : Notifying Client")
	go func() {
		client, ok := lr.ThisServer.Clients[tsedOp.Op]
		if !ok {
			return
		}

		ok = call(client, "Client.RequestDone", &tsedOp.Op, new(interface{}))
		if ok {
			debug("[*] Info : Learner : Notified Client")
		}
	}()
}

/** Learn prev operations from previous paxos instances**/
func (lr *Learner) catchUp(tsedOp TSedOp) {
	//Check to see if op has been learned
	if _, ok := lr.LearnedOps[tsedOp.Op.OpID.OpSeq]; ok {
		return
	}
	//Learn It
	lr.LearnedOps[tsedOp.Op.OpID.OpSeq] = tsedOp

	//Reflect Changes make
	lr.ThisServer.OpSeq = tsedOp.Op.OpID.OpSeq + 1

	//Reflect changes in the learner
	lr.ThisServer.tAcceptor.Lock()
	lr.ThisServer.tAcceptor.LatestAcceptdOps = tsedOp
	lr.ThisServer.tAcceptor.Unlock()
	if lr.writer != nil {
		lr.writer.Encode(tsedOp)
	}
}

//Checks if count is majority
func (lr *Learner) majority(count int) bool {
	n := len(lr.ThisServer.GroupInfoPtr.GroupMembers)
	if count >= int(math.Floor(float64((n+1)/2))) {
		return true
	}
	return false
}

type PendingOperation struct {
	op            TSedOp
	count         int
	seenAcceptors []int
}

func (po *PendingOperation) haveSeenAcceptor(SID int) bool {
	for _, sid := range po.seenAcceptors {
		if sid == SID {
			return true
		}
	}
	return false
}
