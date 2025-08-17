package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mkyas/miniraft"
	"google.golang.org/protobuf/proto"
)

const (
	follower  = "Follower"
	candidate = "Candidate"
	leader    = "Leader"
	failed    = "Failed"
)

const (
	heartbeatInterval           = 75
	electionTimeIntervalMin     = 150
	electionTimeIntervalRandMax = 150
	electionTimeoutMin          = 1500
	electionTimeoutRandMax      = 500
	getUpToSpeedTime            = 2000
)

var (
	serverID                string
	servers                 []string
	state                   = follower
	currentTerm             = uint64(0)
	votedFor                = ""
	voteCount               = 0
	mutex                   sync.Mutex
	logMutex                sync.Mutex
	leaderID                = ""
	lastLogIndex            = uint64(0)
	lastLogTerm             = uint64(0)
	prevLogIndex            = uint64(0)
	prevLogTerm             = uint64(0)
	commitIndex             = uint64(0)
	nextIndex               = make(map[string]uint64)
	matchIndex              = make(map[string]uint64)
	electionTimer           *time.Timer
	updateElectionTimerChan = make(chan int)
	leaderDetectElectChan   = make(chan bool, 1)
	leaderElectedChan       = make(chan bool, 1)
	logFilename             string
	listener                *net.UDPConn
	lastApplied             uint64 = 0
	logEntries              []*miniraft.LogEntry
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: go run raftserver.go server-host:server-port servers.txt")
	}

	serverID = os.Args[1]
	configFile := os.Args[2]

	loadServers(configFile)
	if !isServerInConfig(serverID) {
		log.Fatalf("Server ID %s not found in configuration file", serverID)
	}

	addr, err := net.ResolveUDPAddr("udp", serverID)
	if err != nil {
		log.Fatal(err)
	}

	listener, err = net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	if !initiateLogs(serverID) {
		log.Fatalf("Could not initialize log file. Server %s cannot start.", serverID)
	}

	resetElectionTimer()

	go monitorElectionTimer()

	go runServerConsole()

	for {
		handleIncomingMessages(listener)
	}

}

func loadServers(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("❌ Error opening configuration file:", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		server := strings.TrimSpace(scanner.Text())
		if server != "" {
			servers = append(servers, server)
		}
	}
}

func isServerInConfig(id string) bool {
	for _, s := range servers {
		if s == id {
			return true
		}
	}
	return false
}

func initiateLogs(serverID string) bool {
	filename := strings.ReplaceAll(serverID, ":", "-") + ".log"

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	logFilename = filename
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	defer file.Close()

	return true
}

func runServerConsole() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		scanner.Scan()
		command := strings.TrimSpace(scanner.Text())
		switch command {
		case "log":
			printLogEntries()
		case "print":
			printServerState()
		case "suspend":
			suspendServer()
		case "resume":
			resumeServer()
		default:
			fmt.Println("Unknown command")
		}
	}
}

func printLogEntries() {
	logMutex.Lock()
	defer logMutex.Unlock()
	for _, entry := range logEntries {
		fmt.Printf("%d,%d,%s\n", entry.Index, entry.Term, entry.CommandName)
	}
}

func printServerState() {
	mutex.Lock()
	defer mutex.Unlock()

	fmt.Printf("Server State for %s\n", serverID)
	fmt.Printf("Current term: %d\n", currentTerm)
	fmt.Printf("Voted for: %s\n", votedFor)
	fmt.Printf("State: %s\n", state)
	fmt.Printf("Commit index: %d\n", commitIndex)
	fmt.Printf("Last applied index: %d\n", lastLogIndex)
	fmt.Println("Next index per peer:")
	for peer, idx := range nextIndex {
		fmt.Printf("  %s: %d\n", peer, idx)
	}
	fmt.Println("Match index per peer:")
	for peer, idx := range matchIndex {
		fmt.Printf("  %s: %d\n", peer, idx)
	}
}

func suspendServer() {
	mutex.Lock()
	defer mutex.Unlock()
	if state != failed {
		state = failed
		fmt.Printf("Server %s suspended\n", serverID)
	}
}

func resumeServer() {
	mutex.Lock()
	defer mutex.Unlock()
	if state == failed {
		state = follower
		resetElectionTimer()
		fmt.Printf("Server %s resumed as follower\n", serverID)
	}
}

func handleIncomingMessages(listener *net.UDPConn) {
	data := make([]byte, 1024)
	n, addr, err := listener.ReadFromUDP(data)
	if err != nil {
		return
	}

	if n == 0 {
		return
	}

	var raftMsg miniraft.Raft
	if err := proto.Unmarshal(data[:n], &raftMsg); err != nil {
		return
	}

	switch msg := raftMsg.Message.(type) {
	case *miniraft.Raft_RequestVoteRequest:
		handleRequestVote(msg.RequestVoteRequest, addr.String())

	case *miniraft.Raft_RequestVoteResponse:
		handleVoteResponse(msg.RequestVoteResponse)

	case *miniraft.Raft_AppendEntriesRequest:
		handleAppendEntriesRequests(msg.AppendEntriesRequest, addr.String())

	case *miniraft.Raft_AppendEntriesResponse:
		handleAppendEntriesResponse(msg.AppendEntriesResponse, addr.String())

	case *miniraft.Raft_CommandName:
		handleCommandFromClient(msg.CommandName, addr.String())

	default:
	}
}

func monitorElectionTimer() {
	time.Sleep(getUpToSpeedTime * time.Millisecond) // This timer is for the server to get up to speed -> the server has time to detect the leader so it wont triger straight away an election
	for {
		mutex.Lock()
		timer := electionTimer
		mutex.Unlock()

		if timer != nil {
			select {
			case <-timer.C:
				mutex.Lock()
				if state == follower {
					mutex.Unlock()
					startElection()
				} else {
					mutex.Unlock()
				}
			case <-updateElectionTimerChan:
			}
		} else {
			mutex.Lock()
			if state != failed {
				mutex.Unlock()
				startElection()
			} else {
				mutex.Unlock()
			}
		}
	}
}

func resetElectionTimer() {
	if electionTimer != nil {
		electionTimer.Stop()
	}

	electionTimer = getRandomElectionTimer()

	select {
	case updateElectionTimerChan <- 1:
	default:
	}
}

func getRandomElectionTimer() *time.Timer {
	randomDuration := time.Duration(electionTimeIntervalMin+rand.Intn(electionTimeIntervalRandMax)) * time.Millisecond
	return time.NewTimer(randomDuration)
}

func startElection() {
	mutex.Lock()

	if state == leader || state == failed {
		mutex.Unlock()
		return
	}

	state = candidate
	currentTerm++
	votedFor = serverID
	voteCount = 1
	mutex.Unlock()

	voteRequest := &miniraft.RequestVoteRequest{
		Term:          currentTerm,
		LastLogIndex:  lastLogIndex,
		LastLogTerm:   lastLogTerm,
		CandidateName: serverID,
	}

	raftMsg := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteRequest{RequestVoteRequest: voteRequest},
	}

	data, err := proto.Marshal(raftMsg)
	if err != nil {
		return
	}

	for _, peer := range servers {
		if peer != serverID {
			go func(p string) {
				sendMessage(p, data)
			}(peer)
		}
	}

	electionTimeout := time.After(time.Duration(electionTimeoutMin+rand.Intn(electionTimeoutRandMax)) * time.Millisecond)

	select {
	case <-electionTimeout:
		mutex.Lock()
		if state == candidate {
			state = follower
			votedFor = ""
			resetElectionTimer()
		}
		mutex.Unlock()

	case <-leaderElectedChan:
		return

	case <-leaderDetectElectChan:
		mutex.Lock()
		if state != failed {
			state = follower
		}
		votedFor = ""
		resetElectionTimer()
		mutex.Unlock()
		return
	}
}

func handleRequestVote(req *miniraft.RequestVoteRequest, sender string) {
	mutex.Lock()
	if state == failed {
		mutex.Unlock()
		return
	}
	defer mutex.Unlock()

	if req.Term > currentTerm {
		currentTerm = req.Term
		state = follower
		leaderID = ""
		votedFor = ""
	}

	grantVote := false

	if req.Term >= currentTerm {
		upToDate := (req.LastLogTerm > lastLogTerm) || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

		if (votedFor == "" || votedFor == req.CandidateName) && upToDate {
			grantVote = true
			votedFor = req.CandidateName
			resetElectionTimer()
		}
	}

	voteResponse := &miniraft.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: grantVote,
	}

	raftMsg := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteResponse{RequestVoteResponse: voteResponse},
	}

	data, _ := proto.Marshal(raftMsg)

	sendMessage(sender, data)

}

func handleVoteResponse(res *miniraft.RequestVoteResponse) {
	mutex.Lock()
	if state == failed {
		mutex.Unlock()
		return
	}
	defer mutex.Unlock()

	if res.Term > currentTerm {
		currentTerm = res.Term
		votedFor = ""
		state = follower
		return
	}

	if res.Term == currentTerm && state == candidate {
		if res.VoteGranted {
			voteCount++
		}

		if voteCount >= (len(servers)/2)+1 {
			select {
			case leaderElectedChan <- true:
			default:
			}
			state = leader
			leaderID = serverID

			for _, peer := range servers {
				nextIndex[peer] = lastLogIndex + 1
				matchIndex[peer] = 0
			}

			go sendHeartbeats()
		}
	}
}

func sendHeartbeats() {
	for {
		mutex.Lock()
		if state != leader {
			mutex.Unlock()
			return
		}

		for _, peer := range servers {
			if peer == serverID {
				continue
			}

			next := nextIndex[peer]
			var entries []*miniraft.LogEntry

			if lastLogIndex < next {
				entries = []*miniraft.LogEntry{}
			} else {
				entries = getLogEntriesFrom(next)
			}

			var prevLogIndex uint64 = 0
			if next > 1 {
				prevLogIndex = next - 1
			}

			var prevLogTerm uint64 = 0
			if prevLogIndex > 0 {
				prevEntry := getLogEntryByIndex(prevLogIndex)
				if prevEntry != nil {
					prevLogTerm = prevEntry.Term
				}

			}

			appendEntries := &miniraft.AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     serverID,
				LeaderCommit: commitIndex,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
			}

			raftMsg := &miniraft.Raft{
				Message: &miniraft.Raft_AppendEntriesRequest{AppendEntriesRequest: appendEntries},
			}

			data, _ := proto.Marshal(raftMsg)
			sendMessage(peer, data)

		}

		mutex.Unlock()
		time.Sleep(heartbeatInterval * time.Millisecond)
	}
}

func getLogEntriesFrom(startIndex uint64) []*miniraft.LogEntry {
	logMutex.Lock()
	defer logMutex.Unlock()
	var entries []*miniraft.LogEntry

	for _, entry := range logEntries {
		if entry.Index >= startIndex {
			entries = append(entries, &miniraft.LogEntry{
				Term:        entry.Term,
				Index:       entry.Index,
				CommandName: entry.CommandName,
			})
		}
	}
	return entries
}

func handleAppendEntriesRequests(req *miniraft.AppendEntriesRequest, sender string) {
	mutex.Lock()
	defer mutex.Unlock()

	if state == failed {
		return
	}

	if req.Term < currentTerm {
		sendAppendEntrieRespond(false, req.LeaderId)
		return
	}

	if req.Term > currentTerm {
		currentTerm = req.Term
		state = follower
		leaderID = req.LeaderId
		votedFor = ""
	}

	if state == candidate {
		state = follower
		votedFor = ""
		select {
		case <-leaderDetectElectChan:
		default:
		}
		leaderDetectElectChan <- true
	}

	success := true

	if req.PrevLogIndex > 0 {
		prevEntry := getLogEntryByIndex(req.PrevLogIndex)
		if prevEntry == nil || prevEntry.Term != req.PrevLogTerm {
			success = false
		}
	}

	if success {
		for _, entry := range req.Entries {
			existing := getLogEntryByIndex(entry.Index)
			if existing != nil {
				if existing.Term != entry.Term {
					deleteConflictingEntriesFromMemory(entry.Index)
					break
				} else {
					continue
				}
			}
			appendToMemoryLog(entry.Term, entry.Index, entry.CommandName)
		}
	}

	sendAppendEntrieRespond(success, req.LeaderId)

	if req.LeaderCommit > commitIndex {
		newCommitIndex := req.LeaderCommit
		if lastLogIndex < newCommitIndex {
			newCommitIndex = lastLogIndex
		}
		if newCommitIndex > commitIndex {
			commitIndex = newCommitIndex
			commitEntriesUpToIndex(commitIndex)
		}
	}

	resetElectionTimer()
	leaderID = req.LeaderId
}

func commitEntriesUpToIndex(targetIndex uint64) {
	logMutex.Lock()
	defer logMutex.Unlock()

	file, err := os.OpenFile(logFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	for lastApplied < targetIndex {
		entry := logEntries[lastApplied]
		line := fmt.Sprintf("%d,%d,%s\n", entry.Term, entry.Index, entry.CommandName)
		if _, err := file.WriteString(line); err != nil {
			return
		}
		lastApplied++
	}
}

func getLogEntryByIndex(index uint64) *miniraft.LogEntry {
	logMutex.Lock()
	defer logMutex.Unlock()
	for _, entry := range logEntries {
		if entry.Index == index {
			return entry
		}
	}
	return nil
}

func deleteConflictingEntriesFromMemory(conflictIndex uint64) {
	logMutex.Lock()
	defer logMutex.Unlock()
	var newLogEntries []*miniraft.LogEntry
	for _, entry := range logEntries {
		if entry.Index < conflictIndex {
			newLogEntries = append(newLogEntries, entry)
		}
	}
	logEntries = newLogEntries
	if len(logEntries) > 0 {
		lastLogIndex = logEntries[len(logEntries)-1].Index
		lastLogTerm = logEntries[len(logEntries)-1].Term
	} else {
		lastLogIndex = 0
		lastLogTerm = 0
	}
}

func sendAppendEntrieRespond(success bool, sendTo string) bool {
	if state == failed {
		return false
	}
	appendEntrieResponse := &miniraft.AppendEntriesResponse{
		Term:    currentTerm,
		Success: success,
	}

	raftMsg := &miniraft.Raft{
		Message: &miniraft.Raft_AppendEntriesResponse{AppendEntriesResponse: appendEntrieResponse},
	}

	data, _ := proto.Marshal(raftMsg)

	return sendMessage(sendTo, data)
}

func handleAppendEntriesResponse(res *miniraft.AppendEntriesResponse, sender string) {
	mutex.Lock()
	defer mutex.Unlock()

	if state != leader {
		return
	}

	if res.Term > currentTerm {
		state = follower
		currentTerm = res.Term
		votedFor = ""
		return
	}

	if res.Success {
		nextIndex[sender] = lastLogIndex + 1
		matchIndex[sender] = nextIndex[sender] - 1
	} else {
		if nextIndex[sender] > 1 {
			nextIndex[sender]--
		}
		return
	}

	tryAdvanceCommitIndex()
}

func tryAdvanceCommitIndex() {
	N := lastLogIndex

	for i := N; i > commitIndex; i-- {
		count := 1
		for _, peer := range servers {
			if peer != serverID {
				if matchIndex[peer] >= i {
					count++
				}
			}
		}
		if count > len(servers)/2 && getLogEntryByIndex(i).Term == currentTerm {
			commitIndex = i
			break
		}
	}
	flushCommittedEntriesToFile()
}

func flushCommittedEntriesToFile() {
	logMutex.Lock()
	defer logMutex.Unlock()

	file, err := os.OpenFile(logFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	for lastApplied < commitIndex {
		entry := logEntries[lastApplied]
		line := fmt.Sprintf("%d,%d,%s\n", entry.Term, entry.Index, entry.CommandName)
		if _, err := file.WriteString(line); err != nil {
			return
		}
		lastApplied++
	}
}

func handleCommandFromClient(command string, sender string) {
	mutex.Lock()
	defer mutex.Unlock()

	if state == candidate {
		return
	}

	if state == follower || state == failed {
		raftMsg := &miniraft.Raft{
			Message: &miniraft.Raft_CommandName{CommandName: command},
		}

		data, _ := proto.Marshal(raftMsg)
		sendMessage(leaderID, data)
	}

	if state == leader {
		lastLogIndex++
		appendToMemoryLog(currentTerm, lastLogIndex, command)
		matchIndex[serverID] = lastLogIndex
		nextIndex[serverID] = lastLogIndex + 1
	}

	return
}

func appendToMemoryLog(term uint64, index uint64, command string) {
	logMutex.Lock()
	defer logMutex.Unlock()
	entry := miniraft.LogEntry{
		Term:        term,
		Index:       index,
		CommandName: command,
	}
	logEntries = append(logEntries, &entry)
	lastLogIndex = index
	lastLogTerm = term
}

func sendMessage(target string, data []byte) bool {
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return false
	}

	_, err = listener.WriteToUDP(data, addr)
	if err != nil {
		return false
	}

	return true
}
