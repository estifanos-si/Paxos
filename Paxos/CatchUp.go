package Paxos

type CatchUp struct {
	ThisServer *ThisServer
}

/*Ask serv for the operations that have been learned
 * between ThisServer.OpSeq an d highestOpSeq
 */
func NewCatchup(ts *ThisServer) (cu *CatchUp) {
	cu = new(CatchUp)
	cu.ThisServer = ts
	return
}

func (c *CatchUp) getOperationsPr(serv string, highestOpSeq int) bool {
	//Check if this server has learned all ops that are learned
	if c.ThisServer.OpSeq >= highestOpSeq {
		//It is upto date
		return true
	} else {
		//Haven't seen some operations need to catch up
		c.getOperations(serv)
		//Check if Everything Went well
		if c.ThisServer.OpSeq >= highestOpSeq {
			//It is upto date
			return true
		}

		return false
	}
}

func (c *CatchUp) getOperationsAc(serv string, highestOpSeq int) bool {
	if c.ThisServer.OpSeq == highestOpSeq {
		//It is upto date
		return true
	} else if highestOpSeq > c.ThisServer.OpSeq {
		//Haven't seen some operations need to catch up
		c.getOperations(serv)
		if c.ThisServer.OpSeq == highestOpSeq {
			//if finally caught up
			return true
		}
		return false
	}
	return false
}
func (c *CatchUp) getOperationsLr(serv string, highestOpSeq int) bool {
	//Just call getOperationsAc, They have the same logic
	return c.getOperationsAc(serv, highestOpSeq)
}
func (c *CatchUp) getOperations(serv string) {
	//Get operations to fill in the gap created between
	//This servers OpSeq and Highest Opseq observed from other servers
	//This happens if the leader proposer (or an acceptor) have been disconnected
	//and then rejoined but have missed operations
	if serv == c.ThisServer.ServerAddress {
		return
	}
	lastSeenOp := c.ThisServer.OpSeq - 1
	var reply CatchUpRes
	if ok := call(serv, "CatchUp.SendOps", &lastSeenOp, &reply); !ok {
		debug_err("[*] Error : Contacting Server : %s For updates Failed.", serv)
	}
	for _, op := range reply.LearnedOps {
		c.ThisServer.tLearner.catchUp(op)
	}
}

/*Remote RPC method used for sending missing operations, upon the request of any server*/
func (c *CatchUp) SendOps(lastSeenOp *int, reply *CatchUpRes) error {
	lrndOps := c.ThisServer.tLearner.LearnedOps
	for i := *lastSeenOp + 1; i < len(lrndOps)+1; i++ {
		//Send The Operation
		reply.LearnedOps = append(reply.LearnedOps, lrndOps[i])
	}
	return nil
}
