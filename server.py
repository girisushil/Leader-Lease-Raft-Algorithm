import sys
import os
from threading import Thread, Timer
from concurrent import futures
import random
import json
import grpc
import time
import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc

ID = int(sys.argv[1])
SERVERS_INFO = {}


class Server:
    def __init__(self):
        """
        Initialize a new Server object.

        :param id: An integer representing the unique ID of the server.
        """
        self.state = "Follower"  # Follower state by default
        self.term = 0  # Current term
        self.id = ID  # Unique server ID
        self.voted = False  # Whether the server has voted in the current term
        self.leaderid = -1  # ID of the current leader (-1 if there is none)
        # Whether the server is in sleep mode (for testing purposes)
        self.sleep = False
        self.timeout = None  # Randomized timeout period
        self.timer = None  # Timer object for managing timeouts
        # List of threads (for voting and sending heartbeats)
        self.threads = []
        self.votes = []  # List of received votes
        self.commitIndex = 0  # Index of highest log entry known to be committed
        self.lastApplied = 0  # Index of highest log entry applied to state machine
        self.database = {}  # Key-value store representing server's state machine
        self.log = []  # List of log entries
        self.nextIndex = []  # For each server, index of the next log entry to send to that server
        # For each server, index of highest log entry known to be replicated on server
        self.matchIndex = []
        self.count=0
        self.metadata={}
        # for leader lease initialization
        self.lease=None
        self.lease_duration=5
        self.expire_lease_time=None
        self.max_lease_timer=[]
        self.follower_store_lease=None
        self.start()

    # Function to write data to a file in JSON format
    def write_to_file(self,data, filename,choice):
        if choice:
            with open(filename, 'a') as f:
                json.dump(data, f)
        else:
            with open(filename, 'w') as f:
                json.dump(data, f)

    # Function to read data from a JSON file
    def read_from_file(self,filename):
        with open(filename, 'r') as f:
            data = json.load(f)
            return data

    def stepdown_leader(self):
        self.state="Follower"
        self.leaderid=-1
        self.write_to_file(f"{self.id} Stepping down",f"logs_Node_{self.id}/dump.txt", 1)
        self.restart_timer(self.timeout, self.follower_action)

    def max_lease_time_finder(self):
        a=-1
        for i in range(len(self.max_lease_timer)):
            if(a<self.max_lease_timer[i]):
                a=self.max_lease_timer[i]
        return a

    def initaliations(self):
        data=self.read_from_file(f"logs_Node_{self.id}/logs.txt")
        self.log=data
        data2=self.read_from_file(f"logs_Node_{self.id}/metadata.txt")
        self.commitIndex=data2["CommitLength"]
        self.term=data2["Term"]

    def start(self):
        """
        Starts the server by setting a timeout and creating a callback timer for a follower
        """
        self.set_timeout()
        self.timer = Timer(0, self.follower_declaration)
        self.timer.start()

    def set_timeout(self):
        """
        Set a new timeout for the server, and stores it in the state.
        """
        if self.sleep:
            return

        self.timeout = random.uniform(0.150, .300)

    def restart_timer(self, time, func):
        """
        Restart the server's timer with a new period and callback function.

        :param time: An integer representing the period of the timer.
        :param func: A function to be called when the timer expires.
        """
        self.timer.cancel()
        self.timer = Timer(time, func)
        self.timer.start()

    def update_state(self, state):
        """
        Update the current state of the server.

        :param state: A string representing the new state.
        """
        if self.sleep:
            return

        self.state = state
        print(f'Term: {self.term}\t State: {self.state}')

    def update_term(self, term):
        """
        Update the current term of the server.

        :param term: An integer representing the new term.
        """
        if self.sleep:
            return

        self.voted = False
        self.term = term
        print(
            f'\n-+-+-+-+-+- Term: {self.term} -+-+-+-+-+-\n')

    def follower_declaration(self):
        """
        Method for declaring the server to be in follower state
        """
        if self.sleep:
            return

        self.update_state("Follower")
        self.write_to_file(self.log, f"logs_Node_{self.id}/logs.txt",0)
        self.restart_timer(self.timeout, self.follower_action)

    def follower_action(self):
        """
        Called upon not receiving heartbeats (leader is dead).
        """
        if self.sleep or self.state != "Follower":
            return

        print(f'Term: {self.term}\t Leader is dead')
        self.leaderid = -1
        self.write_to_file(self.log, f"logs_Node_{self.id}/logs.txt",0)
        self.candidate_declaration()

    def candidate_declaration(self):
        """
        Method for declaring the server to be a candidate for leader election
        """
        if self.sleep:
            return

        self.update_term(self.term+1)
        self.update_state("Candidate")
        self.voted = True
        self.leaderid = self.id

        print(f'Term: {self.term}\t Voted_For: {self.id}')

        self.write_to_file(self.log, f"logs_Node_{self.id}/logs.txt",0)
        self.restart_timer(self.timeout, self.candidate_action)
        self.candidate_election()

    def candidate_election(self):
        """
        Initiate a new candidate election.
        """
        if self.sleep or self.state != "Candidate":
            return
        self.write_to_file(f"Node {self.id} election timer timed out, Starting election.",f"logs_Node_{self.id}/dump.txt", 1)
        self.votes = [0 for _ in range(len(SERVERS_INFO))]
        self.threads = []
        for k, v in SERVERS_INFO.items():
            if k == ID:
                self.votes[k] = 1
                continue
            self.threads.append(Thread(target=self.request, args=(k, v)))
        for t in self.threads:
            t.start()

    def candidate_action(self):
        """
        Perform the candidate's action.
        """
        if self.sleep or self.state != "Candidate":
            return

        for t in self.threads:
            t.join(0)

        print(
            f"Term: {self.term}\t Votes_Recieved: {sum(self.votes)}/{len(self.votes)}")

        if sum(self.votes) > (len(self.votes)//2):
            self.timeout = 0.050
            self.write_to_file(self.log, f"logs_Node_{self.id}/logs.txt",0)
            self.leader_declaration()
        else:
            self.set_timeout()
            self.write_to_file(self.log, f"logs_Node_{self.id}/logs.txt",0)
            self.follower_declaration()

    def leader_declaration(self):
        """
        Declare the server as the new leader.
        """
        if self.sleep:
            return

        if self.max_lease_time_finder()>time.time():# for checking the control
            print("The lease is with other leader")
        self.update_state("Leader")
        self.lease = True  # for acquiring lease
        self.leaderid = self.id
        self.write_to_file(f"Node {self.leaderid} became the leader for term {self.term}",
                               f"logs_Node_{self.id}/dump.txt", 1)
        self.count += 1
        if (self.count == 1):
            self.SetNoOp()
            self.count += 1
        self.nextIndex = [(len(self.log) + 1) for i in SERVERS_INFO]
        self.matchIndex = [0 for i in SERVERS_INFO]
        self.leader_action()


    def SetNoOp(self):
        """
        Method to handle No Op operqation.

        :param request: A KeyValMessage object containing the key and value to be set.
        :param context: The context of the request.
        """
        reply = {"success": False}

        if self.state == "Leader":

            self.log.append({"term": self.term, "update": {
                "command": "NO-OP", "key": None,
                "value": None}})
            reply = {"success": True}
        elif self.state == "Follower":
            channel = grpc.insecure_channel(f'{SERVERS_INFO[self.leaderid]}')
            stub = pb2_grpc.ServiceStub(channel)
            message = pb2.KeyValMessage(key=None, value=None)
            try:
                response = stub.SetVal(message)
                reply = {"success": response.success}
            except grpc.RpcError:
                print("Server is not avaliable")

    def leader_action(self):
        """
        Sends heartbeats to followers.
        """
        if self.sleep or self.state != "Leader":
            return
        self.expire_lease_time=time.time()+self.lease_duration
        self.threads = []
        self.write_to_file(f"Leader {self.leaderid} sending heartbeat & Renewing Lease",f"logs_Node_{self.id}/dump.txt",1)
        for k, v in SERVERS_INFO.items():
            if k == ID:
                continue
            self.threads.append(Thread(target=self.heartbeat, args=(k, v)))
        for t in self.threads:
            t.start()
        self.restart_timer(self.timeout, self.leader_check)

    def leader_check(self):
        """
        Checks the database for commits and updates accordingly.
        """
        if self.sleep or self.state != "Leader":
            return

        # if self.expire_lease_time<time.time():
        #     self.stepdown_leader()

        for t in self.threads:
            t.join(0)

        self.nextIndex[ID] = len(self.log)+1
        self.matchIndex[ID] = len(self.log)

        commits = sum(
            1 for element in self.matchIndex if element >= self.commitIndex+1)

        if commits > int(len(self.matchIndex)//2):
            self.commitIndex += 1
        while self.commitIndex > self.lastApplied:
            key, value = self.log[self.lastApplied]["update"]["key"], self.log[self.lastApplied]["update"]["value"]
            self.database[key] = value
            self.write_to_file(f"Node {self.leaderid} (leader) committed the entry SET {key} {value} to the state machine.",
                               f"logs_Node_{self.id}/dump.txt", 1)
            print(f'Term: {self.term}\t {key} = {value}')

            self.lastApplied += 1
        self.write_to_file(self.log,f"logs_Node_{self.id}/logs.txt",0)
        self.leader_action()

    def request(self, id, address):
        """
        Sends a requestVote to a server.

        :param id: An integer representing the id of the server.
        :param address: A string representing the address of the server.
        """
        if self.sleep or self.state != "Candidate":
            return

        channel = grpc.insecure_channel(address)
        stub = pb2_grpc.ServiceStub(channel)
        message = pb2.RequestTermIdMessage(term=int(self.term), id=int(self.id), last_log_index=len(
            self.log), last_log_term=(0 if len(self.log) == 0 else self.log[-1]["term"]))
        try:
            response = stub.RequestVote(message)
            reciever_term = response.term
            reciever_result = response.result
            receiver_lease_expire_time=response.time_lease
            self.max_lease_timer.append(receiver_lease_expire_time)
            if reciever_term > self.term:
                self.update_term(reciever_term)
                self.set_timeout()
                self.follower_declaration()
            elif reciever_result:
                self.votes[id] = 1
        except grpc.RpcError:
            return

    def heartbeat(self, id, address):
        """
        Sends a heartbeat to a server.

        :param id: The id of the server to send the heartbeat to.
        :param address: The address of the server to send the heartbeat to.
        """
        if self.sleep or (self.state != "Leader"):
            return

        channel = grpc.insecure_channel(address)
        stub = pb2_grpc.ServiceStub(channel)

        entries = []
        if self.nextIndex[id] <= len(self.log):
            entries = [self.log[self.nextIndex[id]-1]]

        prev_log_term = 0
        if self.nextIndex[id] > 1:
            prev_log_term = self.log[self.nextIndex[id]-2]["term"]

        message = pb2.AppendTermIdMessage(term=int(self.term), id=int(
            self.id), prev_log_index=self.nextIndex[id]-1, prev_log_term=prev_log_term, entries=entries, leader_commit=self.commitIndex,expire_Lease=int(self.expire_lease_time))

        try:
            response = stub.AppendEntries(message)
            reciever_term = response.term
            reciever_result = response.result
            if reciever_term > self.term:
                self.update_term(reciever_term)
                self.set_timeout()
                self.follower_declaration()
            else:
                if reciever_result:
                    if len(entries) != 0:
                        self.matchIndex[id] = self.nextIndex[id]
                        self.nextIndex[id] += 1
                else:
                    self.nextIndex[id] -= 1
                    self.matchIndex[id] = min(
                        self.matchIndex[id], (self.nextIndex[id]-1))
        except grpc.RpcError:
            self.write_to_file(f"Error occurred while sending RPC to Node {id}",
                               f"logs_Node_{self.id}/dump.txt", 1)
            return

    def gotosleep(self, period):
        """
        Method to suspend the server for a given period of time.

        :param period: The period of time in seconds to suspend the server for.
        """
        self.sleep = True
        print(f'Term: {self.term}\t Sleeping for {period} seconds')
        self.restart_timer(int(period), self.wakeup)

    def wakeup(self):
        """
        Method to wake up the server after being suspended.
        """
        self.sleep = False
        self.follower_declaration()


class Handler(pb2_grpc.ServiceServicer, Server):
    def __init__(self):
        super().__init__()

    def RequestVote(self, request, context):
        """
        Method to handle a requestVote from a server.

        :param request: A RequestTermIdMessage object containing the term, id, last_log_index and last_log_term of the server.
        :param context: The context of the request.
        """
        if self.sleep:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.TermResultMessage()

        reply = {"term": -1, "result": False,"time_lease":0}

        if request.term == self.term:  # In the same term as me, we both are or were candidates
            if self.voted or request.last_log_index < len(self.log) or self.state != "Follower":
                reply = {"term": int(self.term), "result": False,"time_lease":0}
                self.write_to_file(f"Vote denied for Node {request.id} in term {self.term}.",
                                   f"logs_Node_{self.id}/dump.txt", 1)
            elif request.last_log_index == len(self.log):
                if self.log[request.last_log_index-1]["term"] != request.last_log_term:
                    reply = {"term": int(self.term), "result": False,"time_lease":0}
                    self.write_to_file(f"Vote denied for Node {request.id} in term {self.term}.",
                                       f"logs_Node_{self.id}/dump.txt", 1)
            else:
                self.voted = True
                self.leaderid = request.id
                self.metadata["Term"]=self.term
                self.metadata["NodeID"]=request.id
                self.metadata["CommitLength"]=self.commitIndex
                self.write_to_file(self.metadata, f"logs_Node_{self.id}/metadata.txt",0)
                print(f'Term: {self.term}\t Voted_For: {request.id}')
                self.write_to_file(f"Vote granted for Node {request.id} in term {self.term}.",
                                   f"logs_Node_{self.id}/dump.txt", 1)

                reply = {"term": int(self.term), "result": True,"time_lease":self.follower_store_lease}

            if self.state == "Follower":
                self.restart_timer(self.timeout, self.follower_action)

        elif request.term > self.term:  # I am in an earlier term
            self.update_term(request.term)
            self.leaderid = request.id
            self.metadata["Term"] = self.term
            self.metadata["NodeID"] = request.id
            self.metadata["CommitLength"] = self.commitIndex
            self.write_to_file(self.metadata, f"logs_Node_{self.id}/metadata.txt",0)
            print(f'Term: {self.term}\t Voted_For: {request.id}')
            self.write_to_file(f"Vote granted for Node {request.id} in term {self.term}.",
                               f"logs_Node_{self.id}/dump.txt", 1)
            self.leaderid = request.id
            self.voted = True
            self.follower_declaration()
            reply = {"term": int(self.term), "result": True,"time_lease":self.follower_store_lease}

        else:  # Candidate is in an earlier term
            reply = {"term": int(self.term), "result": False,"time_lease":0}
            self.write_to_file(f"Vote denied for Node {request.id} in term {self.term}.",
                               f"logs_Node_{self.id}/dump.txt", 1)
            if self.state == "Follower":
                self.restart_timer(self.timeout, self.follower_action)

        return pb2.TermResultMessage(**reply)

    def AppendEntries(self, request, context):
        """
        Method to handle an appendEntries from a server.

        :param request: An AppendTermIdMessage object containing the term, id, prev_log_index, prev_log_term, entries and leader_commit of the server.
        :param context: The context of the request.
        """
        if self.sleep:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.TermResultMessage()
        reply = {"term": -1, "result": False,"time_lease":0}

        if request.term >= self.term:
            if request.term > self.term:
                self.write_to_file(f"Node {self.id} rejected AppendEntries RPC from {self.leaderid}.",
                                   f"logs_Node_{self.id}/dump.txt", 1)
                self.update_term(request.term)
                self.follower_declaration()
                self.leaderid = request.id

            if len(self.log) < request.prev_log_index:
                reply = {"term": int(self.term), "result": False,"time_lease":0}
                self.write_to_file(f"Node {self.id} rejected AppendEntries RPC from {self.leaderid}.",
                                   f"logs_Node_{self.id}/dump.txt", 1)
                if self.state == "Follower":
                    self.restart_timer(self.timeout, self.follower_action)

            else:
                if len(self.log) > request.prev_log_index:
                    self.log = self.log[:request.prev_log_index]

                if len(request.entries) != 0:
                    self.write_to_file(
                        f"Node {self.id} accepted AppendEntries RPC from {self.leaderid}.",
                        f"logs_Node_{self.id}/dump.txt", 1)
                    self.follower_store_lease=request.expire_Lease
                    self.log.append({"term": request.entries[0].term, "update": {
                                    "command": request.entries[0].update.command, "key": request.entries[0].update.key, "value": request.entries[0].update.value}})

                if request.leader_commit > self.commitIndex:
                    self.commitIndex = min(
                        request.leader_commit, len(self.log))
                    while self.commitIndex > self.lastApplied:
                        key, value = self.log[self.lastApplied]["update"]["key"], self.log[self.lastApplied]["update"]["value"]
                        self.database[key] = value
                        self.write_to_file(f"Node {request.id} (follower) committed the entry SET {key} {value} to the state machine",f"logs_Node_{self.id}/dump.txt", 1)
                        self.follower_store_lease = request.expire_Lease
                        print(f'Term: {self.term}\t {key} = {value}')
                        self.lastApplied += 1

                reply = {"term": int(self.term), "result": True,"time_lease":0}
                self.follower_store_lease = request.expire_Lease
                self.restart_timer(self.timeout, self.follower_action)
        else:  # Requester is in an earlier term
            reply = {"term": int(self.term), "result": False,"time_lease":0}
            self.write_to_file(f"Node {self.id} rejected AppendEntries RPC from {self.leaderid}.",f"logs_Node_{self.id}/dump.txt", 1)

        return pb2.TermResultMessage(**reply)

    def Suspend(self, request, context):
        """
        Method to handle a suspend request from a server.

        :param request: An EmptyMessage object.
        :param context: The context of the request.
        """
        if self.sleep:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.EmptyMessage()

        period = request.period
        reply = {}

        # print(f'Term: {self.term}\t Command: suspend {period}')
        self.gotosleep(period)

        return pb2.EmptyMessage(**reply)

    def GetLeader(self, request, context):
        """
        Method to handle a getleader request from a server.

        :param request: An EmptyMessage object.
        :param context: The context of the request.
        """
        if self.sleep:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.LeaderMessage()

        reply = {"leader": -1, "address": ""}

        # print(f'Term: {self.term}\t Command: getleader')

        if self.leaderid != -1:  # I have a leader
            reply = {"leader": int(self.leaderid),
                     "address": SERVERS_INFO[self.leaderid]}

        return pb2.LeaderMessage(**reply)

    def SetVal(self, request, context):
        """
        Method to handle a setval request from a server.

        :param request: A KeyValMessage object containing the key and value to be set.
        :param context: The context of the request.
        """
        if self.sleep:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.SuccessMessage

        reply = {"success": False}
        self.write_to_file(f"Node {self.leaderid} (leader) received an SET {request.key} {request.value} request.",
                           f"logs_Node_{self.id}/dump.txt", 1)
        if self.state == "Leader":
            if (self.count == 1):
                self.log.append({"term": self.term, "update": {
                    "command": "NO-OP", "key": None,
                    "value": None}})
            self.log.append({"term": self.term, "update": {
                            "command": 'set', "key": request.key, "value": request.value}})
            reply = {"success": True}
        elif self.state == "Follower":
            channel = grpc.insecure_channel(f'{SERVERS_INFO[self.leaderid]}')
            stub = pb2_grpc.ServiceStub(channel)
            message = pb2.KeyValMessage(key=request.key, value=request.value)
            try:
                response = stub.SetVal(message)
                reply = {"success": response.success}
            except grpc.RpcError:
                print("Server is not avaliable")

        return pb2.SuccessMessage(**reply)




    def GetVal(self, request, context):
        """
        Method to handle a getval request from a server.

        :param request: A KeyMessage object containing the key to be retrieved.
        :param context: The context of the request.
        """
        if self.sleep:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.SuccessValMessage

        reply = {"success": False, "value": "None"}
        self.write_to_file(f"Node {self.leaderid} (leader) received a GET {request.key} request.",
                           f"logs_Node_{self.id}/dump.txt", 1)
        if request.key in self.database:
            reply = {"success": True, "value": self.database[request.key]}

        return pb2.SuccessValMessage(**reply)

    def GetStatus(self, request, context):
        """
        Method to handle a getstatus request from a server.

        :param request: An EmptyMessage object.
        :param context: The context of the request.
        """
        if self.sleep:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.EmptyMessage()

        reply = {}

        # print(f'Term: {self.term}\t Command: getStatus')

        return pb2.EmptyMessage(**reply)


def serve():
    """
    Method to start the server.
    """
    print(f'Server ID: {ID}')
    print(f'Server Address: {SERVERS_INFO[ID]}')
    print(f'================================\n')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ServiceServicer_to_server(Handler(), server)
    server.add_insecure_port(SERVERS_INFO[ID])
    try:
        server.start()
        while True:
            server.wait_for_termination()
    except grpc.RpcError:
        print("Unexpected Error")
        os._exit(0)
    except KeyboardInterrupt:
        print("Shutting Down")
        os._exit(0)


def configuration():
    """
    Setup configuration of servers based on Config.conf
    """
    with open('Config.conf') as f:
        global SERVERS_INFO
        lines = f.readlines()
        for line in lines:
            parts = line.split()
            id, address, port = parts[0], parts[1], parts[2]
            SERVERS_INFO[int(id)] = (f'{str(address)}:{str(port)}')


def main():
    """
    Main function for calling all phases of the code
    """
    configuration()
    serve()


if __name__ == "__main__":
    main()
