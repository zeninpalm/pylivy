import re
from enum import Enum
from functools import total_ordering


@total_ordering
class Version:
    def __init__(self, version):
        match = re.match(r"(\d+)\.(\d+)\.(\d+)(\S*)$", version)
        if match is None:
            raise ValueError("invalid version string %s" % version)
        self.major, self.minor, self.dot, self.extension = match.groups()

    def __repr__(self):
        name = self.__class__.__name__
        return "%s(%s.%s.%s%s)" % (name, self.major, self.minor, self.dot, self.extension)

    def __eq__(self, other):
        return (
            isinstance(other, Version)
            and self.major == other.major
            and self.minor == other.minor
            and self.dot == other.dot
        )

    def __lt__(self, other):
        if self.major < other.major:
            return True
        elif self.major == other.major:
            if self.minor < other.minor:
                return True
            elif self.minor == other.minor:
                return self.dot < other.dot
            else:
                return False
        else:
            return False


class SparkRuntimeError(Exception):
    def __init__(
        self,
        ename,
        evalue,
        traceback,
    ):
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback

    def __repr__(self):
        name = self.__class__.__name__
        components = []
        if self.ename is not None:
            components.append("ename=%s" % self.ename)
        if self.evalue is not None:
            components.append("evalue=%s" % self.evalue)
        return "%s(%s)" % (name, ", ".join(components))


class OutputStatus(Enum):
    OK = "ok"
    ERROR = "error"


class Output:
    def __init__(self, status, text, json, ename, evalue, traceback):
        self.status = status
        self.text = text
        self.json = json
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback

    @classmethod
    def from_json(cls, data):
        return cls(
            OutputStatus(data["status"]),
            data.get("data", {}).get("text/plain"),
            data.get("data", {}).get("application/json"),
            data.get("ename"),
            data.get("evalue"),
            data.get("traceback"),
        )

    def raise_for_status(self):
        if self.status == OutputStatus.ERROR:
            raise SparkRuntimeError(self.ename, self.evalue, self.traceback)

    def __eq__(self, other):
        return (
            isinstance(other, Output)
            and self.status == other.status
            and self.text == other.text
            and self.json == other.json
            and self.ename == other.ename
            and self.evalue == other.evalue
            and self.traceback == other.traceback
        )


class StatementKind(Enum):
    SPARK = "spark"
    PYSPARK = "pyspark"
    SPARKR = "sparkr"
    SQL = "sql"


class StatementState(Enum):
    WAITING = "waiting"
    RUNNING = "running"
    AVAILABLE = "available"
    ERROR = "error"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class Statement:
    def __init__(self, session_id, statement_id, state, output):
        self.session_id = session_id
        self.statement_id = statement_id
        self.state = state
        self.output = output

    @classmethod
    def from_json(cls, session_id, data):
        if data["output"] is None:
            output = None
        else:
            output = Output.from_json(data["output"])
        return cls(
            session_id, data["id"], StatementState(data["state"]), output
        )

    def __eq__(self, other):
        return (
            isinstance(other, Statement)
            and self.session_id == other.session_id
            and self.statement_id == other.statement_id
            and self.state == other.state
            and self.output == other.output
        )


class SessionKind(Enum):
    SPARK = "spark"
    PYSPARK = "pyspark"
    PYSPARK3 = "pyspark3"
    SPARKR = "sparkr"
    SQL = "sql"
    SHARED = "shared"


# Possible session states are defined here:
# https://github.com/apache/incubator-livy/blob/master/core/src/main/scala/
# org/apache/livy/sessions/SessionState.scala
class SessionState(Enum):
    NOT_STARTED = "not_started"
    STARTING = "starting"
    RECOVERING = "recovering"
    IDLE = "idle"
    RUNNING = "running"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"


SESSION_STATE_NOT_READY = {SessionState.NOT_STARTED, SessionState.STARTING}
SESSION_STATE_FINISHED = {
    SessionState.ERROR,
    SessionState.DEAD,
    SessionState.KILLED,
    SessionState.SUCCESS,
}


class Session:
    def __init__(self, session_id, proxy_user, kind, state):
        self.session_id = session_id
        self.proxy_user = proxy_user
        self.kind = kind
        self.state = state
        
    @classmethod
    def from_json(cls, data):
        return cls(
            data["id"],
            data["proxyUser"],
            SessionKind(data["kind"]),
            SessionState(data["state"]),
        )
        
    def __eq__(self, other):
        return (self.session_id == other.session_id) and \
            (self.proxy_user == other.proxy_user) and \
            (self.kind == other.kind) and \
            (self.state == other.state)


class Batch:
    def __init__(self, batch_id, app_id, app_info, log, state):
        self.batch_id = batch_id
        self.app_id = app_id
        self.app_info = app_info
        self.log = log
        self.state = state
        
    @classmethod
    def from_json(cls, data):
        return cls(
            data["id"],
            data.get("appId"),
            data.get("appInfo"),
            data.get("log", []),
            SessionState(data["state"]),
        )

    def __eq__(self, other):
        return (self.batch_id == other.batch_id) and \
            (self.app_id == other.app_id) and \
            (self.app_info == other.app_info) and \
            (self.log == other.log) and \
            (self.state == other.state)

class BatchLog:
    def __init__(self, batch_id, from_, total, lines):
        self.batch_id = batch_id
        self.from_ = from_
        self.total = total
        self.lines = lines
        
    @classmethod
    def from_json(cls, data):
        return cls(
            data["id"], data["from"], data["total"], data.get("log", [])
        )
        
    def __eq__(self, other):
        return (self.batch_id == other.batch_id) and \
            (self.from_ == other.from_) and \
            (self.total == other.total) and \
            (self.lines == other.lines)
