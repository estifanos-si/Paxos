package Paxos

import (
	"encoding/json"
	"sync"
)

type Acceptor struct {
	sync.Mutex

	LatestAcceptdOps TSedOp
	HighestSeenTS    TimeStamp
	ThisServer       *ThisServer
}

func NewAcceptor(thisServer *ThisServer) (ac *Acceptor) {
	ac = new(Acceptor)
	ac.ThisServer = thisServer
	ac.HighestSeenTS = TimeStamp{-1, 0}
	ac.LatestAcceptdOps = TSedOp{}
	ac.LatestAcceptdOps.Op.OpID.SID = -1
	ac.LatestAcceptdOps.Op.OpID.OpSeq = -1
	ac.LatestAcceptdOps.Op.Value = NO_OP
	ac.LatestAcceptdOps.OpTs = TimeStamp{-1, -1}
	return
}

/*This is a remote method, A proposer performing a prepare calls this method
 *Prepare(ts) msg from our textbook
 */
func (ac *Acceptor) Prepare(pMsg *PrepareMsg, promise *Promise) error {
	ac.ThisServer.Lock()
	ac.Lock()
	defer ac.Unlock()
	defer ac.ThisServer.Unlock()

	promise.PrevAcceptedOp.Value = NO_OP
	promise.PrevAcceptedOp.OpID.OpSeq = -1
	promise.OpTS = TimeStamp{-1, -1}
	promise.HasPromised = false

	if ac.HighestSeenTS.Less(&pMsg.TS) {
		//pMsg has a timestamp greater than any seen, make promise,
		ac.HighestSeenTS = pMsg.TS
		promise.HasPromised = true
		promise.PrevAcceptedOp = ac.LatestAcceptdOps.Op
		promise.OpTS = ac.LatestAcceptdOps.OpTs
		str, _ := json.Marshal(pMsg.TS)
		debug("[*] Info : Acceptor : Promising To Timestamp %s ", str)
	}
	return nil
}

/*Remote Method Called by A Leading proposer
 * Accept(ts, op) message from our textbook
 */
func (ac *Acceptor) Accept(operation *TSedOp, reply *interface{}) error {
	//Need to make sure the acceptor doesnt accept new operations until last one in queue
	//is learned
	ac.Lock()
	if !operation.OpTs.Less(&ac.HighestSeenTS) {
		//Highest TS seen so far
		//check if need to catch up
		ac.Unlock()
		originServ := ac.ThisServer.GroupInfoPtr.GroupMembers[operation.OpTs.SID]
		ac.ThisServer.Lock()
		ac.ThisServer.tLearner.Lock()
		if ac.ThisServer.tCu.getOperationsAc(originServ, operation.Op.OpID.OpSeq) {
			//Upto date can go ahead and accept
			ac.ThisServer.tLearner.Unlock()
			ac.ThisServer.Unlock()
			ac.Lock()
			ac.LatestAcceptdOps = *operation
			ac.Unlock()
			str, _ := json.Marshal(operation)
			debug("[*] Info : Acceptor : Accepted Operation %s ", str)
			op := *operation
			go ac.sendLearn(op)
			return nil
		} else {
			ac.ThisServer.tLearner.Unlock()
			ac.ThisServer.Unlock()
			debug("[*] This Server Not Upto date Aborting ")
		}

		return nil
	}
	ac.Unlock()
	return nil
}

func (ac *Acceptor) sendLearn(operation TSedOp) {
	//Send learn for learners!
	for _, serv := range ac.ThisServer.GroupInfoPtr.GroupMembers {
		go func(serv string) {
			var ok bool
			lrnMsg := LearnMsg{operation, ac.ThisServer.SID}
			if serv == ac.ThisServer.ServerAddress {
				ac.ThisServer.tLearner.Learn(&lrnMsg, new(interface{}))
				debug("[*] Info : Acceptor : Learner.Learn of Server %s Returned", serv)
				ok = true
			} else {
				ok = call(serv, "Learner.Learn", &lrnMsg, new(interface{}))
				debug("[*] Info : Acceptor : Learner.Learn of Server %s Returned", serv)
			}
			if !ok {
				debug_err("Error: Acceptor : Learner.Learn for server %s Failed ", serv)
			}
		}(serv)
	}
}
