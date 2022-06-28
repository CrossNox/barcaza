import abc
import asyncio
import json
import random
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union

import zmq


@dataclass
class LogEntry:
    term: int
    command: str
    index: int

    def to_json(self):
        return json.dumps(
            {"term": self.term, "command": self.command, "index": self.index}
        )

    @classmethod
    def from_json(cls, jsonstring):
        parsed_result = json.loads(jsonstring)
        return LogEntry(**parsed_result)


class ServerStatus:
    # Persistent state
    term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = []

    # Volatile state
    commit_index: int = 0
    last_applied: int = 0

    @property
    def last_log_entry(self):
        try:
            return self.log[-1]
        except IndexError:
            return None


class RPCError(Exception):
    def __init__(self, recvd_string, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.recvd_string = recvd_string


class ServerComms:
    def __init__(self, timeout, port):
        self.timeout = (random.random() + 1) * timeout
        self.assign_random_timeout(timeout)
        self.port = port

        # Communication
        self.context = zmq.sugar.context.Context.instance()

        self.rep = self.context.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{self.port}")
        self.rep.RCVTIMEO = self.timeout

        self.peers = {}

    def recv_string(self):
        return self.rep.recv_string()

    def send_string(self, string):
        return self.rep.send_string(string)

    def recv_rpc(self, *clss):
        recvd_string = self.recv_string()
        for cls in clss:
            try:
                return cls.from_json(recvd_string)
            except:
                pass
        raise RPCError(recvd_string)

    def send_rpc(self, rpc):
        self.send_string(rpc.to_json())

    @property
    def npeers(self):
        return len(self.peers)

    def add_peer(self, identifier: str, host: str, port: int):
        if identifier in self.peers:
            raise ValueError(f"peer {identifier} is already registered")
        self.peers[identifier] = Peer(identifier, host, port)


@dataclass
class VoteResult:
    term: int
    granted: bool

    def to_json(self):
        return json.dumps({"term": self.term, "granted": self.granted})

    @classmethod
    def from_json(cls, jsonstring):
        parsed_result = json.loads(jsonstring)
        return VoteResult(**parsed_result)


@dataclass
class RequestVote:
    candidate_id: str
    term: int
    last_log_entry: LogEntry

    def to_json(self):
        return json.dumps(
            {
                "term": self.term,
                "candidate_id": self.candidate_id,
                "last_log_entry": self.last_log_entry.to_json(),
            }
        )

    @classmethod
    def from_json(cls, jsonstring):
        parsed_result = json.loads(jsonstring)
        return RequestVote(**parsed_result)


@dataclass
class AppendEntries:
    term: int
    leader_id: str
    prev_log_entry: LogEntry
    entries: List[str]
    leader_commit: int

    @property
    def empty(self):
        return len(self.entries) == 0

    def to_json(self):
        return json.dumps(
            {
                "term": self.term,
                "leader_id": self.leader_id,
                "prev_log_entry": self.prev_log_entry.to_json(),
                "entries": self.entries,
                "leader_commit": self.leader_commit,
            }
        )

    @classmethod
    def from_json(cls, jsonstring):
        parsed_result = json.loads(jsonstring)
        return AppendEntries(**parsed_result)


@dataclass
class AppendEntriesResult:
    term: int
    success: bool

    def to_json(self):
        return json.dumps({"term": self.term, "success": self.success})

    @classmethod
    def from_json(cls, jsonstring):
        parsed_result = json.loads(jsonstring)
        return AppendEntriesResult(**parsed_result)


@abc.ABC
class Status:
    def __init__(self, identifier: str, status: ServerStatus, comms: ServerComms):
        self.identifier = identifier
        self.status = status
        self.comms = comms

    @abc.abstractmethod
    def _step(self):
        pass

    def transition(self, cls):
        return cls(self.identifier, self.status, self.comms)

    def check_term(self, rpc: Union[RequestVote, AppendEntries]):
        if rpc.term > self.status.term:
            return self.to_follower()
        return None

    def step(self):
        if self.status.commit_index > self.status.last_applied:
            self.status.apply_last()
        self._step()


class AppendEntryFromNewLeader(Exception):
    pass


class ElectionTimeout(Exception):
    pass


class Follower(Status):
    def handle_heartbeat(self, heartbeat):
        raise NotImplementedError()

    def _step(self):
        try:
            append_entry = self.comms.recv_rpc(AppendEntries)
            if append_entry.empty():
                self.handle_heartbeat(append_entry)
            else:
                raise NotImplementedError()
        except zmq.error.ZMQError as e:
            if e.errno != zmq.EAGAIN:
                raise
            # Timed out, start election
            return self.transition(Candidate)


class Candidate(Status):
    def issue_request_votes(self):
        pass

    async def issue_request_votes(self) -> List[VoteResult]:
        vote_requests_futures = [
            peer.request_vote(self.identifier, self.term, self.last_log_entry)
            for peer in self.peers
        ]
        vote_results = await asyncio.gather(*vote_requests_futures)
        return vote_results

    def _step(self):
        self.status.term += 1
        # TODO: reset election timer?
        try:
            votes = self.issue_request_votes()
            total_votes = len(votes) + 1
            positive_votes = sum(vote.granted for vote in votes) + 1
            if positive_votes / total_votes >= 0.5:
                return self.transition(Leader)
        except AppendEntryFromNewLeader:
            return self.transition(Follower)
        except ElectionTimeout:
            return self


class Leader(Status):
    def __init__(self, just_elected: bool = True):
        self.just_elected = just_elected

    async def send_heartbeats(self):
        heartbeats = [
            peer.send_append_entry(
                term=self.status.term,
                leader_id=self.identifier,
                prev_log_entry=self.status.last_log_entry,
                leader_commit=self.status.commit_index,
                entries=[],
            )
            for peer in self.comms.peers
        ]
        heartbeats_results = await asyncio.gather(*heartbeats)
        return heartbeats_results

    def apply(self, entry):
        # append entry to local log
        # apply to state machine

        # if last log index >= next_index for a follower
        # send AppendEntries withlog entries starting at next_index

        # If successful: update nextIndex and matchIndex for follower
        # If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
        raise NotImplementedError()

    def respond(self, entry):
        raise NotImplementedError()

    def _step(self):
        if self.just_elected:
            self.send_heartbeats()

        self.just_elected = False
        try:
            entry = self.comms.recv_string()
            self.apply(entry)
            self.respond(entry)

            # If there exists an N such that N > commitIndex, a majority
            # of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            # set commitIndex = N

        except zmq.error.ZMQError as e:
            if e.errno != zmq.EAGAIN:
                raise
            self.send_heartbeats()


@dataclass
class Peer:
    identifier: str
    host: str
    port: int

    def __post__init__(self):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{self.host}:{self.port}")

    async def request_vote(self, identifier, term, last_log_entry) -> VoteResult:
        req_vote = RequestVote(identifier, term, last_log_entry)
        await self.socket.send_string(req_vote.to_json())
        response = await self.socket.recv_string()
        return VoteResult.from_json(response)

    async def send_append_entry(
        self,
        term: int,
        leader_id: str,
        prev_log_entry: LogEntry,
        leader_commit: int,
        entries: List[str] = [],
    ) -> AppendEntriesResult:
        append_entry = AppendEntries(
            term, leader_id, prev_log_entry, entries, leader_commit
        )
        await self.socket.send_string(append_entry.to_json())
        response = await self.socket.recv_string()
        return AppendEntriesResult.from_json(response)


@dataclass
class Server:
    timeout: int
    port: int
    identifier: str

    def __post__init__(self):
        self.status = ServerStatus()
        self.comms = ServerComms(self.timeout, self.port)
        self.state = Follower(self.identifier, self.status, self.comms)

    def log_up_to_date(self, vote_request: RequestVote):
        if (
            vote_request.last_log_entry.index == self.status.last_log_entry.index
            and vote_request.last_log_entry.term == self.status.last_log_entry.term
        ):
            return True

        if vote_request.last_log_entry.term > self.status.last_log_entry.term:
            return True

        if (
            vote_request.last_log_entry.term == self.status.last_log_entry.term
            and vote_request.last_log_entry.index > self.status.last_log_entry.index
        ):
            return True

        return False

    def handle_vote_request(self, vote_request: RequestVote):
        if vote_request.term < self.status.term:
            peer = self.peers[vote_request.candidate_id]
            peer.reply_vote_request(VoteResult(0, False))
        elif (
            self.status.voted_for is not None
            and self.status.voted_for != vote_request.candidate_id
        ):
            peer.reply_vote_request(VoteResult(0, False))
        elif not self.log_up_to_date(vote_request):
            peer.reply_vote_request(VoteResult(0, False))
        else:
            peer.reply_vote_request(VoteResult(vote_request.term, True))

    def apply_last(self):
        raise NotImplementedError()

    def start(self):
        while True:
            self.state.step()
