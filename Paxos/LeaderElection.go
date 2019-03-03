package Paxos

import (
	"fmt"
	"sync"
	"time"
)

/*This Module implements The Bully Leader Election Algorithm.
 *A leader Election algorithm isn't neccessary for paxos, but it
 *enhances the LIVENESS property of paxos, by trying to make sure that
 *there's only one leader at any one time, among the majorities.
 */
type LeaderElector struct {
	sync.Mutex

	LeaderSID      int
	CurrentLeader  string        //The current leaders SID:Address
	PollLeaderFreq time.Duration // time interval to poll leader status in secs
	ThisServer     *ThisServer
	RegainLeadFreq time.Duration
	adjustingLead  bool
}

func NewLeaderElector(CurrentLeader string, LeaderSID int,
	pollLdFrq time.Duration, rgnLdFrq time.Duration, thsSrv *ThisServer) (le *LeaderElector) {
	le = new(LeaderElector)
	le.CurrentLeader = CurrentLeader
	le.LeaderSID = LeaderSID
	le.ThisServer = thsSrv
	le.PollLeaderFreq = pollLdFrq
	le.RegainLeadFreq = rgnLdFrq
	le.adjustingLead = false
	return
}

/*Check if current leader is alive,or suspect it to have failed*/
func (le *LeaderElector) PollLeader() {
	//Ping leader repeatdly every 15sec, if hasnt responded for 2 consequent pings, init LeaderChange
	//The frequency can be changed in paxos.config.json file.
	if le.LeaderSID != NO_LEADER && le.LeaderSID != le.ThisServer.SID {
		alive := false
		stillLeader := false
		for i := 0; i < 2; i++ {
			ok := call(le.CurrentLeader, "LeaderElector.Alive", new(interface{}), &stillLeader)
			if ok && stillLeader {
				alive = true
				break
			}
		}
		if !alive {
			debug("[*] Info : LeaderElector : Leader with SID : %d && address : %s Suspected  To have Failed. Starting Leader Election.",
				le.LeaderSID, le.CurrentLeader)
			le.initElection()
		}
	} else if le.LeaderSID == NO_LEADER && !le.adjustingLead {
		le.initElection()
	}
	//Wait for 15 or specified secs
	<-time.After(le.PollLeaderFreq * time.Second)
	le.PollLeader()
}

//Remote method used for polling the current leader
func (le *LeaderElector) Alive(_ *interface{}, stillLeader *bool) error {
	if le.LeaderSID == le.ThisServer.SID {
		*stillLeader = true
		return nil
	}
	*stillLeader = false
	return nil
}

func (le *LeaderElector) Stabilize() {
	<-time.After(le.PollLeaderFreq * 10 * time.Second)
	le.initElection()
	le.Stabilize()
}

/*Tries to find the current leader, this happens if this server was a leader and
got disconnected,and then rejoined*/
func (le *LeaderElector) adjustLeadership() {
	//Try to regain leadership if this is the server with highest rank
	//or discover server with highest rank
	le.Lock()
	le.adjustingLead = true
	le.CurrentLeader = ""
	le.LeaderSID = NO_LEADER
	le.Unlock()
	<-time.After(le.RegainLeadFreq * time.Second)
	debug("[*] Info : LeaderElector : Adjusting LeaderShip Election Started.")
	le.initElection()
	le.Lock()
	le.adjustingLead = false
	le.Unlock()
}

//Have suspected the leader to have failed, initialize leader election
func (le *LeaderElector) initElection() {
	le.Lock()
	le.CurrentLeader = ""
	le.LeaderSID = NO_LEADER
	le.Unlock()
	highestRank := false
	//Poll servers with higher rank
	for SID, serv := range le.ThisServer.GroupInfoPtr.GroupMembers {
		if SID < le.ThisServer.SID {
			//Has Higher rank, SID 0 > SID 1 > SID 2 ....
			ok := call(serv, "LeaderElector.ChangeLeader", new(interface{}), &highestRank)
			if ok && highestRank == true {
				//Theres a server with higher rank, let go
				debug("[*] Info : LeaderElector : There is Another Server - %s- With Higher Rank.Backing off. ", serv)
				return
			}
		}
	}
	//No server with higher rank, become leader
	le.becomeLeader()
}

/*If there is no other server with higher rank become leader*/
func (le *LeaderElector) becomeLeader() {
	done := make(chan int)
	highestTS := -1
	lock := new(sync.Mutex)
	//Notify Other Servers,and also poll for the highest TS seen
	for SID, serv := range le.ThisServer.GroupInfoPtr.GroupMembers {
		ts := -1
		if SID > le.ThisServer.SID {
			if ok := call(serv, "LeaderElector.NotifyLeaderChange", &le.ThisServer.SID, &ts); !ok {
				debug_err("Error : %s %s", "LeaderElector.NotifyLeaderChange Failed For Server :", serv)
				continue
			}
			lock.Lock()
			if ts > highestTS {
				highestTS = ts
			}
			lock.Unlock()
		}
	}
	le.makeTSAdjustments(highestTS, &done)
	debug("[*] Info : This server is The Leader.")
}

func (le *LeaderElector) makeTSAdjustments(highestTS int, done *chan int) {
	//Make sure the leader has seen the highest TimeStamp, Needed to make progress

	le.ThisServer.tProposer.Lock()
	if highestTS != -1 && highestTS >= le.ThisServer.tProposer.CurrentTS {
		le.ThisServer.tProposer.CurrentTS = highestTS + 1
	}
	le.ThisServer.tProposer.Unlock()
	//No Server with higher rank is alive this is the leader
	le.Lock()
	le.CurrentLeader = le.ThisServer.ServerAddress
	le.LeaderSID = le.ThisServer.SID
	le.Unlock()
}

/*This is an RPC method, called during leader election when
 *a server with lower rank checks if any servers with higher
 *ranks are alive
 */
func (le *LeaderElector) ChangeLeader(_ *interface{}, highestRank *bool) error {
	go le.initElection()
	*highestRank = true
	return nil
}

//Leader after winning election calls on every other server to accept it as the new leader
func (le *LeaderElector) NotifyLeaderChange(SID *int, highestSeenTS *int) error {
	le.Lock()
	if le.LeaderSID != NO_LEADER {
		stillLeader := false
		ok := call(le.CurrentLeader, "LeaderElector.Alive", new(interface{}), &stillLeader)
		if le.LeaderSID > *SID && ok && stillLeader {
			//No need to change Leader, already connedted to the highest ranked leader
			*highestSeenTS = -1
			return nil
		}
	}
	le.Unlock()
	//There is no leader or leader isnt reachable or found a new server with higher rank than leader
	le.ThisServer.tAcceptor.Lock()
	*highestSeenTS = le.ThisServer.tAcceptor.HighestSeenTS.SeqNum
	le.ThisServer.tAcceptor.Unlock()
	le.Lock()
	le.LeaderSID = *SID
	le.CurrentLeader = le.ThisServer.GroupInfoPtr.GroupMembers[*SID]
	debug("[*] Changed Leader TO %d -- %s", le.LeaderSID, le.CurrentLeader)
	fmt.Printf("")
	le.Unlock()
	return nil
}
