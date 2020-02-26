# Distributed Systems Programing Final Project  

## Description

    This is an implementaion of the paxos consensus algorithm using GO. 
    Implemented in this project is the core paxos algorithm, along with multi-paxos
    algorithm, which is based on the paxos algorithm but has additional functionalities.
    The multi-paxos algorithm uses multiple instances of a single paxos algorithm to implement a replicated
    state machine (or achieve state machine replication).

## Launch

    To run the paxos implementation, there needs to be atleast threee different instances of the program. These
    instances don't neccessarily have to be on three different machines. One can emulate three different instances by
    running each program with different port address on the same machine. As long as the three
    (or possibly more) processes have unique addresses and server ids(SIDs), the implementation works correctly.
    The paxos.config.json file contains configurations such as the mapping of SID's to Server addresses and other details.
    The client.config.json file contains configuration for the client such as who the leader is,
    the list of paxos servers to contact if the leader fails.

### Steps are as follows (for running on the same machine)

    1. cd <Project root>
    2. Set The PAXOS_SID enviroment variable to a unique number.The Server Id of the process is determined by this.
    Make sure the SID'S set match the SID'S in the paxos.config.json file
        export PAXOS_SID=`The unique Server ID`

    3. set the GOPATH to the project root.
        export GOPATH=$(pwd)

    4. launch the program.
        cd src
        go run paxos_server.go

    5. Repeat the above step for additional two (or more) servers, on different terminals.
    6. After all the servers are up, run the client on a separate terminal (or Tab):
        6.1 set the GOPATH to the project root.
            export GOPATH=$(pwd)
        6.2 cd src/Paxos_Client
        6.3 go run paxos_client.go
        6.4 submit an operation

### Steps For running the processes on different machines

    The steps are the same but there is no need to set the PAXOS_SID
    enviroment variable as it can be configured on the different paxos.config.json file on each server.
    The servers unique SID can be read from the paxos.config.json file. Make sure the different config files have
    different SID'S for each server instance.

## Failure Handling

    The paxos implementation given can handle multiple server failures including the leader. As long as the majority of
    the servers are correct and can communicate they can make progres.

## Modules and files included

    1. The Paxos package contains all the modules needed.
    2. Proposer.go, Acceptor.go, Learner.go programs implement all the neccessary steps of their respective components in
    the paxos algorithm.
    3. The LeaderElection.go program implements a variant of The bully leader election algorithm. This is needed to improve
    the LIVENESS of paxos.
    4. The CatchUp.go program provides an implementation to fill in missing learned operations incase of network partition. 
    It fetches missing learned operations from other serveres, and requested by other servers provide its own learned operations
    5. The Paxos_client package contains code for the client.

    Paxos assumes crash failures therefore there is no need to keep logs. That is if a server fails, it is assumed it doesn't restart. 
    The log files are provided so that comparison of the order in which the servers
    executed/learned the operations can be carried out.

    With a simple modification the log files can be  used so that if a server is restarted it can continue from where it left of.
