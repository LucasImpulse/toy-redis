from re import T
import grpc
from concurrent import futures
import time
import argparse
import os, sys
import random
import threading
import json

# generated code
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.insert(0, root_dir)
import raft_pb2
import raft_pb2_grpc

# engine
import flashkv

# states of the nodes
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"

# We make insecure connections here because for localhost being insecure is fine.
# Why would you sniff this with Wireshark when you could just open the source and the db files?
# Obv TLS in real deployment but complaints on it doing this on localhost will be "acknowledged".

class RaftServer(raft_pb2_grpc.RaftNodeServicer):

    def __init__(self, db_path, node_id, peers):
        self.node_id = node_id
        self.peers = peers    # list of peer node IDs
        print(f"--- Node {node_id} initializing storage: {db_path} ---")
        self.db = flashkv.Database(db_path)

        self.current_leader_id = None
        self.commit_lock = threading.Lock()

        # INIT STATE
        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None

        self.load_log()
        self.commit_index = 0
        
        # TIMEOUT = ELECTION
        self.last_heartbeat = time.time()
        self.timeout_duration = random.uniform(2.0, 4.0)    # TODO: later when all is debugged, reduce these times

        # background
        self.running = True
        self.timer_thread = threading.Thread(target=self.run_election_loop)
        self.timer_thread.daemon = True
        self.timer_thread.start()

    # ELECTION THINGS BELOW HERE

    def run_election_loop(self):
        """Thread in background that checks if leader is dead"""
        print(f"[{self.node_id}] Starting Raft Loop (Timeout: {self.timeout_duration:.2f}s)")

        while self.running:
            time.sleep(0.1)

            # I am a LEADER, I don't await my death, my heart beats (and resets timeouts).
            if self.state == LEADER:
                self.send_heartbeats()
                time.sleep(0.2)
                continue

            # I am NOT a LEADER, check for timeout.
            time.sleep(0.1)
            elapsed = time.time() - self.last_heartbeat
            if elapsed > self.timeout_duration:
                self.start_election()

    def send_heartbeats(self):
        """Send empty AppendEntries to all peers."""
        for peer_port in self.peers:
            self.send_append_entries(peer_port)

    def send_append_entries(self, peer_port, key=None, value=None):
        """send AppendEntries RPC to peer, with optional key/value to replicate"""

        # where is peer?
        if peer_port not in self.next_index:
            self.next_index[peer_port] = len(self.raft_log)

        ni = self.next_index[peer_port]
        
        if ni >= len(self.raft_log):
            ni = len(self.raft_log)
            self.next_index[peer_port] = ni

        # derive pointers from ni
        prev_log_index = ni - 1
        if prev_log_index < 0: prev_log_index = 0
        
        prev_log_term = self.raft_log[prev_log_index]['term']

        # entries to send, if backtracking, might be sending old entry, not new kv, so slice from ni onwards.
        entries = self.raft_log[ni:]

        entry_key = ""
        entry_value = ""

        if len(entries) > 0:
            entry_key = entries[0]['key']
            entry_value = entries[0]['value']
        elif key:
            # if triggered by Set() but ni at end, send new entry
            entry_key = key
            entry_value = value

        address = f'localhost:{peer_port}'
        try:
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftNodeStub(channel)

                # if no key/value, it's just a heartbeat
                request = raft_pb2.EntryArgs(
                    term=self.current_term,
                    leaderId=self.node_id,
                    prevLogIndex = prev_log_index,
                    prevLogTerm = prev_log_term,
                    leaderCommit = self.commit_index,
                    key=entry_key,
                    value=entry_value
                )

                # don't block main thread
                response = stub.AppendEntries(request, timeout=0.05)

                # update trackers on response
                if response.success:
                    # advance the pointers
                    if len(entries) > 0 or (key and value):
                        self.next_index[peer_port] = ni + 1
                        self.match_index[peer_port] = ni
                        self.update_commit_index()
                else:
                    # damn it
                    if response.term > self.current_term:
                        # step down, am old
                        self.state = FOLLOWER
                        self.current_term = response.term
                        self.voted_for = None
                    else:
                        # MISMATCH, go back, also, if they rejected 0 stay at 1 (prevLogIndex = 0)
                        new_ni = response.conflictIndex
                        print(f"[{self.node_id}] Backtracking Node {peer_port} to index {new_ni}")
                        self.next_index[peer_port] = new_ni

        except grpc.RpcError as e:
            # print(f"[{self.node_id}] RPC Failed to Node {peer_port}: {e}")
            pass

    def replicate_to_peers(self, key, value):
        """Send data to all followers right way"""
        for peer_port in self.peers:
            self.send_append_entries(peer_port, key, value)

    def start_election(self):
        """Become candidate and ask for votes"""
        print(f"[{self.node_id}] TIMEOUT! No heartbeats in {self.timeout_duration:.2f}s")
        print(f"[{self.node_id}] I am becoming a CANDIDATE, this is round {self.current_term + 1}")

        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id       # stand up for myself
        self.last_heartbeat = time.time()

        self.timeout_duration = random.uniform(2.0, 4.0)

        votes_received = 1   # vote for self

        for peer_port in self.peers:
            # i parallelise later maybe
            vote_granted = self.send_request_vote(peer_port)
            if vote_granted:
                votes_received += 1
                print(f"[{self.node_id}] Received YES vote from Node at port {peer_port}")

        print(f"[{self.node_id}] Election results: {votes_received} votes")

        # simple majority: >50% of nodes
        total_nodes = len(self.peers) + 1
        if votes_received > total_nodes // 2:
            self.become_leader()

    def become_leader(self):
        print(f"[{self.node_id}] *** I AM THE LEADER (Round {self.current_term}) ***")
        self.state = LEADER
        # my heart should beat for other nodes

        # tracking for all peers that match us until they reject us
        last_log_index = len(self.raft_log) - 1

        # track states for each follower later (nextIndex, matchIndex)
        self.next_index = {port: len(self.raft_log) for port in self.peers}
        self.match_index = {port: 0 for port in self.peers}

    def send_request_vote(self, peer_port):
        """Connect and ask for vote from peer"""
        address = f'localhost:{peer_port}'
        try:
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftNodeStub(channel)
                request = raft_pb2.VoteArgs(term=self.current_term, candidateId=self.node_id)
                response = stub.RequestVote(request, timeout=0.1)
                return response.voteGranted

        except:
            # peer probably dead, the dead do not vote
            return False

    def reset_election_timer(self):
        """Reset the election timer (called on receiving valid heartbeat)"""
        self.last_heartbeat = time.time()
        if self.state != FOLLOWER:
            print(f"[{self.node_id}] demoting to FOLLOWER")
            self.state = FOLLOWER

    def update_commit_index(self):
        """if a majority of nodes match an index N, and N > commitIndex, commitIndex = N"""

        with self.commit_lock:
            # grab all match indexes, my own is at end of log
            indices = [len(self.raft_log) - 1]
            for port in self.peers:
                indices.append(self.match_index[port])

            # grab median
            indices.sort()

            # majority index is at len/2
            majority_index = indices[len(indices) // 2]

            # only commit if entry is from current term
            if majority_index > self.commit_index:
                if self.raft_log[majority_index]['term'] == self.current_term:

                    print(f"[{self.node_id}] MAJORITY COMMITTING up to {majority_index}")

                    # all new entries to c++ engine
                    while self.commit_index < majority_index:
                        self.commit_index += 1
                        entry = self.raft_log[self.commit_index]
                        self.db.set(entry['key'], entry['value'])

    def RequestVote(self, request, context):
        """Received vote request from other node"""

        # if malicious node revolts, you ignore as leader is clearly alive
        elapsed = time.time() - self.last_heartbeat
        MIN_ELECTION_TIMEOUT = 1
        if elapsed < MIN_ELECTION_TIMEOUT:
            print(f"[{self.node_id}] Rejecting Vote from Node {request.candidateId}: Leader clearly alive.")
            return raft_pb2.VoteReply(term=self.current_term, voteGranted=False)

        print(f"[{self.node_id}] Received Vote Request from Node {request.candidateId} (Round {request.term})]")

        # if term is older, reject
        if request.term < self.current_term:
            return raft_pb2.VoteReply(term=self.current_term, voteGranted=False)

        # if term is newer, step down and ACK
        if request.term > self.current_term:
            print(f"[{self.node_id}] Seeing higher term {request.term}, stepping down.")
            self.current_term = request.term
            self.state = FOLLOWER
            self.voted_for = None

        # if haven't voted yet, vote for candidate
        if self.voted_for is None or self.voted_for == request.candidateId:
            self.voted_for = request.candidateId
            self.reset_election_timer()
            print(f"[{self.node_id}] Voted YES for Node {request.candidateId}")
            return raft_pb2.VoteReply(term=self.current_term, voteGranted=True)

        # else, already voted for someone else
        print(f"[{self.node_id}] Voted NO for Node {request.candidateId}, already voted for Node {self.voted_for}")
        return raft_pb2.VoteReply(term=self.current_term, voteGranted=False)

    def AppendEntries(self, request, context):
        """Listening for leader's heartbeat, replicating log entries, and committing them"""

        # if leader's term is older, reject
        if request.term < self.current_term:
            return raft_pb2.EntryReply(term=self.current_term, success=False)
        # print(f"[{self.node_id}] Heartbeat from Leader {request.leaderId} (Round {request.term})")    # uncomment to see heartbeat spam
        self.reset_election_timer()
        
        if request.term >= self.current_term:
            # if newer term, submit to new era
            self.current_term = request.term
            self.voted_for = None
            self.state = FOLLOWER

        # consistency check
        # log too short
        if len(self.raft_log) <= request.prevLogIndex:
            print(f"REJECT: Log too short. MyLen: {len(self.raft_log)}, PrevIdx: {request.prevLogIndex}")
            return raft_pb2.EntryReply(
                    term=self.current_term,
                    success=False,
                    conflictIndex=len(self.raft_log)
                )

        # log equal but term mismatch (old leader entry that never got committed)
        if self.raft_log[request.prevLogIndex]['term'] != request.prevLogTerm:
            print(f"REJECT: Term mismatch at {request.prevLogIndex}")
            # everything after this is suspect
            self.raft_log = self.raft_log[:request.prevLogIndex]
            self.persist_log()
            return raft_pb2.EntryReply(
                    term=self.current_term,
                    success=False,
                    conflictIndex=request.prevLogIndex
                )
        # now consistency is OK

        if request.key:
            print(f"[{self.node_id}] REPLICATING {request.key} from Leader {request.leaderId}")
            entry = {"term": request.term, "key": request.key, "value": request.value}
            if len(self.raft_log) == request.prevLogIndex + 1:
                self.raft_log.append(entry)
                self.persist_log()

        if request.leaderCommit > self.commit_index:

            last_new_index = min(request.leaderCommit, len(self.raft_log) - 1)

            while self.commit_index < last_new_index:
                self.commit_index += 1
                entry = self.raft_log[self.commit_index]

                print(f"[{self.node_id}] COMMITTING Index {self.commit_index}: {entry['key']}")
                self.db.set(entry['key'], entry['value'])

        self.current_leader_id = request.leaderId

        return raft_pb2.EntryReply(term=self.current_term, success=True)


    # --- ELECTON FINISHED ---

    # ALL RPC THINGS BELOW HERE

    # Set RPC (leader)
    def Set(self, request, context):
        # request is a KeyValue object (defined in .proto, go look)
        # only leader should write..
        if self.state != LEADER:
            msg = f"NOT LEADER"
            if self.current_leader_id:
                msg += f" (Try Node {self.current_leader_id})"
            return raft_pb2.Status(success=False, message=msg)

        print(f"[{self.node_id}] SET {request.key} (LEADER) - AWAITING MAJORITY NODES")

        entry = {"term": self.current_term, "key": request.key, "value": request.value}
        self.raft_log.append(entry)

        self.persist_log()

        target_index = len(self.raft_log) - 1

        threading.Thread(target=self.replicate_to_peers, args=(request.key, request.value)).start()

        start_wait = time.time()
        while time.time() - start_wait < 2.0:
            if self.commit_index >= target_index:
                return raft_pb2.Status(success=True, message=f"Committed index {target_index}")
            time.sleep(0.01)

        return raft_pb2.Status(success=False, message=f"TIMEOUT: Replication failed")

    # Get RPC (leader)
    def Get(self, request, context):
        # if you are not the leader, tell client who is
        if self.state != LEADER:
            hint = str(self.current_leader_id) if self.current_leader_id else ""
            return raft_pb2.Value(value="",
                                  found=False,
                                  success=False,
                                  leaderHint=hint)

        # only leader can read
        print(f"[{self.node_id}] GET {request.key} (LEADER)")
        try:
            val = self.db.get(request.key)
            return raft_pb2.Value(value=val,
                                  found=True,
                                  success=True,
                                  leaderHint="")
        except RuntimeError:
            # Key not found
            return raft_pb2.Value(value="",
                                  found=False,
                                  success=True,
                                  leaderHint="")

    # --- RPC DONE ---

    # ALL LOG THINGS BELOW HERE

    def load_log(self):
        filename = f"node_{self.node_id}.log.json"
        if os.path.exists(filename):
            try:
                with open(filename, "r") as f:
                    self.raft_log = json.load(f)
                print(f"[{self.node_id}] Loaded Raft Log from disk ({len(self.raft_log)} entries)")
            except Exception as e:
                print(f"[{self.node_id}] Failed to load log: {e}")
                self.raft_log = [{"term": 0, "key": "", "value": ""}]

        else:
            # default empty log
            print("RAH")
            self.raft_log = [{"term": 0, "key": "", "value": ""}]

    def persist_log(self):
        filename = f"node_{self.node_id}.log.json"
        try:
            with open(filename, "w") as f:
                json.dump(self.raft_log, f)
        except Exception as e:
            print(f"[{self.node_id}] Failed to save log: {e}")

    # --- LOG DONE ---

def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", type=str, default="")    # peers are different ports
    args = parser.parse_args()

    # peers to list
    peer_list = [int(p) for p in args.peers.split(",")] if args.peers else []

    # create the server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # register our class with the gRPC machinery
    db_filename = f"node_{args.id}.db"
    raft_server = RaftServer(db_filename, args.id, peer_list)
    raft_pb2_grpc.add_RaftNodeServicer_to_server(raft_server, server)
    
    # listen
    address = f'[::]:{args.port}'
    server.add_insecure_port(address)
    server.start()
    print(f"--- Node {args.id} listening on {address}")
    
    # keep-alive
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()