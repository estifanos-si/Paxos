package Paxos

import (
	"math"
	"sync"
)

type Proposer struct {
	sync.Mutex
	SID        int
	CurrentTS  int
	LastOpSeq  int
	ThisServer *ThisServer
}

/*Initialize The proposer*/
func NewProposer(SID int, thisServ *ThisServer) (pr *Proposer) {

	//Initialize proposers fields
	pr = new(Proposer)
	pr.CurrentTS = 1
	pr.SID = SID
	pr.ThisServer = thisServ
	pr.LastOpSeq = 0
	return
}

/*Send a prepare message for the Acceptors,Recieve promises
 *Subsequently decide wether to send an accept or not
 *if majority of acceptors promise send accept with the
 *value (operation) of the highest proposal TS
 *Called whenever the current leader is suspected to have failed
 */
func (pr *Proposer) sendPrepare() (Promise, int, string, bool) {
	//For all the acceptors that are reachable send prepare(TS), for multi-paxos
	pr.Lock()
	prepMsg := PrepareMsg{TimeStamp{pr.SID, pr.CurrentTS}}
	debug("[*] Info Sending Prepare TS: %v", pr.CurrentTS)
	pr.CurrentTS++
	pr.Unlock()
	lock := new(sync.Mutex)
	//For collecting their respective responses
	var highestPromise Promise
	highestPromise.PrevAcceptedOp.Value = NO_OP
	highestPromise.OpTS = TimeStamp{-1, -1}
	highestOpSeq := -1
	servHighestOpSeq := ""
	promiseCount := 0
	done := make(chan int, len(pr.ThisServer.GroupInfoPtr.GroupMembers))
	for _, serv := range pr.ThisServer.GroupInfoPtr.GroupMembers {
		go func(serv string) {
			//Contact individual acceptors and wait for response -a promise.
			var promise Promise
			var ok bool
			if serv == pr.ThisServer.ServerAddress {
				//Own Acceptor
				pr.ThisServer.tAcceptor.Prepare(&prepMsg, &promise)
				ok = true
			} else {
				ok = call(serv, "Acceptor.Prepare", &prepMsg, &promise)
			}

			if !ok {
				debug_err("Error : %s = %s", "RPC call failed to Acceptor.Prepare, server", serv)
				done <- 1
				return
			}
			debug("[*] Info Server %s  - Replied", serv)
			if promise.HasPromised {
				debug("[*] Info Server %s  - Promised with Promise :%v", serv, promise)
				lock.Lock()
				promiseCount++
				//The server who has seen most operations, needed for catching up
				if highestOpSeq < promise.PrevAcceptedOp.OpID.OpSeq {
					highestOpSeq = promise.PrevAcceptedOp.OpID.OpSeq
					servHighestOpSeq = serv
					//The Acceptor that sent the highest seen TS
					if highestPromise.OpTS.Less(&promise.OpTS) && promise.PrevAcceptedOp.Value != NO_OP {
						highestPromise = promise
					}
				}
				lock.Unlock()
			}
			done <- 1
			return
		}(serv)
	}
	/*Wait until all promises have been collected
	 *Analyze the promise if majority promised, then send accept with
	 *The value of the highest TS
	 */
	for range pr.ThisServer.GroupInfoPtr.GroupMembers {
		<-done
	}

	n := len(pr.ThisServer.GroupInfoPtr.GroupMembers)
	if promiseCount >= int(math.Floor(float64((n+1)/2))) {
		//Got promises from majority of acceptors
		debug("[*] Majority Of Acceptors Promised Prommise Count %v", promiseCount)
		return highestPromise, highestOpSeq, servHighestOpSeq, true
	} else {
		/*There might be another proposer with higher timestamp,
		 *Hence there might be another leader.
		 */
		debug_err("Error : %s ", "Majority did not make a promise, Can't make progress")
		// pr.ThisServer.tLeaderElector.FindLeader()
		return highestPromise, highestOpSeq, servHighestOpSeq, false
	}
}

/*Sends An accept to a majority(or more) of the Acceptors*/
func (pr *Proposer) sendAccept(highestPromise Promise, proposedOp Operation) {
	debug("[*] Sending Accept, Higheset Recieved Promise %v", highestPromise)
	var operation Operation
	if highestPromise.PrevAcceptedOp.Value == NO_OP {
		//Propose own proposal, as there is no other accepted proposal among the majorities
		operation = proposedOp
	} else {
		operation = highestPromise.PrevAcceptedOp
	}
	pr.Lock()
	tsedOp := TSedOp{operation, TimeStamp{pr.SID, pr.CurrentTS - 1}}
	pr.Unlock()
	for _, server := range pr.ThisServer.GroupInfoPtr.GroupMembers {
		var ok bool
		//Send accept to all servers reachable
		go func(server string) {
			if server == pr.ThisServer.ServerAddress {
				pr.ThisServer.tAcceptor.Accept(&tsedOp, new(interface{}))
				ok = true
			} else {
				ok = call(server, "Acceptor.Accept", &tsedOp, new(interface{}))
			}
			if !ok {
				debug_err("Error %s, %s", "RPC call failed, Acceptor.Accept", server)
			}
		}(server)
	}
}

/*If designated Leader, accept a proposal from the other proposers and clients
 *else forward to the Designated Leader
 *This is needed for liveness, and efficiency
 *This is a remote method other Proposers or Clients directly call
 */

func (pr *Proposer) NewOperation(proposedOperation *OperationExReq, reply *OperationExRes) error {
	debug("[*] Info : New Operation Recieved %v", proposedOperation)
	pr.ThisServer.tLeaderElector.Lock()
	if pr.ThisServer.tLeaderElector.LeaderSID == pr.SID {
		//Save Client address to notify it when operation gets "executed"/"Choosen"
		//Assign this operation a sequence number
		pr.ThisServer.tLeaderElector.Unlock()
		debug("[*] Info : Is current Leader Trying To get this operation choosen")
		if pr.onGoingConsensus() {
			debug("[*] There is an Ongoing Consensus, Aborting")
			reply.Succeded = false
			return nil
		}
		highestPromise, highestOpSeq, servHighestOpSeq, ok := pr.sendPrepare()
		if !ok {
			//didnt get majority,Leader might have changed, find it
			go pr.ThisServer.tLeaderElector.adjustLeadership()
			reply.Succeded = false
			return nil
		}

		/*Check if missing Operations,and catchup, (For multi-Paxos)*/
		pr.ThisServer.Lock()
		pr.ThisServer.tLearner.Lock()
		if pr.ThisServer.tCu.getOperationsPr(servHighestOpSeq, highestOpSeq) {
			debug("[*] This Server Is up to date")
			//Have caught up to the most recent operation,so propose this value as a new Operation
			pr.ThisServer.tLearner.Unlock()
			pr.ThisServer.Unlock()
			//Check if trying to propose for a new Paxos instance
			pr.ThisServer.Lock()
			if pr.ThisServer.OpSeq > highestOpSeq {
				//Proposing for a new paxos instance propose own value
				highestPromise.PrevAcceptedOp.Value = NO_OP
			}
			pr.ThisServer.Unlock()

			//Associate client with new opseq
			pr.assignOpSeq(proposedOperation)
		} else {
			debug_err("[Error] : Couldn't get server upto date - Missing Operations! Cant Proceed")
			reply.Succeded = false
			return nil
		}
		//Got Majority, Send accept to Acceptors, With the value of the
		//highest accepted proposal or own proposal if none
		pr.sendAccept(highestPromise, proposedOperation.Operation)

		reply.Succeded = true
		reply.Redirect = false
		return nil
	}
	pr.ThisServer.tLeaderElector.Unlock()

	//Redirect client to the actual leader
	debug("[*] Info : Is Not current Leader Redirecting Client to Leader")
	reply.Succeded = false
	reply.Redirect = true
	reply.LeaderSID = pr.ThisServer.tLeaderElector.LeaderSID
	reply.LeaderAddr = pr.ThisServer.GroupInfoPtr.GroupMembers[reply.LeaderSID]
	return nil
}

func (pr *Proposer) assignOpSeq(proposedOperation *OperationExReq) {
	pr.ThisServer.Lock()
	defer pr.ThisServer.Unlock()

	proposedOperation.Operation.OpID.OpSeq = pr.ThisServer.OpSeq
	proposedOperation.Operation.OpID.SID = pr.SID
	pr.ThisServer.Clients[proposedOperation.Operation] = proposedOperation.ClientAddress
	pr.LastOpSeq = pr.ThisServer.OpSeq
}

func (pr *Proposer) onGoingConsensus() bool {
	pr.ThisServer.Lock()
	pr.Lock()
	defer pr.Unlock()
	defer pr.ThisServer.Unlock()

	if pr.LastOpSeq >= pr.ThisServer.OpSeq {
		//There is an ongoing Consensus
		//There need to only be one operation at a time
		return true
	}
	return false
}
