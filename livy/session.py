import time
import json

import pandas

from livy.client import LivyClient
from livy.models import (
    SessionKind,
    SessionState,
    StatementState,
    Output,
    SESSION_STATE_NOT_READY,
)
from livy.utils import polling_intervals


SERIALISE_DATAFRAME_TEMPLATE_SPARK = "{}.toJSON.collect.foreach(println)"
SERIALISE_DATAFRAME_TEMPLATE_PYSPARK = """
for _livy_client_serialised_row in {}.toJSON().collect():
    print(_livy_client_serialised_row)
"""
SERIALISE_DATAFRAME_TEMPLATE_SPARKR = r"""
cat(unlist(collect(toJSON({}))), sep = '\n')
"""


def serialise_dataframe_code(dataframe_name, session_kind):
    try:
        template = {
            SessionKind.SPARK: SERIALISE_DATAFRAME_TEMPLATE_SPARK,
            SessionKind.PYSPARK: SERIALISE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.PYSPARK3: SERIALISE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.SPARKR: SERIALISE_DATAFRAME_TEMPLATE_SPARKR,
        }[session_kind]
    except KeyError:
        raise RuntimeError(
            "read not supported for sessions of kind %s" % session_kind
        )
    return template.format(dataframe_name)


def deserialise_dataframe(text):
    rows = []
    for line in text.split("\n"):
        if line:
            rows.append(json.loads(line))
    return pandas.DataFrame.from_records(rows)


def dataframe_from_json_output(json_output):
    try:
        fields = json_output["schema"]["fields"]
        columns = [field["name"] for field in fields]
        data = json_output["data"]
    except KeyError:
        raise ValueError("json output does not match expected structure")
    return pandas.DataFrame(data, columns=columns)


class LivySession:
    def __init__(
        self,
        url,
        auth = None,
        verify = True,
        kind = SessionKind.PYSPARK,
        proxy_user = None,
        jars = None,
        py_files = None,
        files = None,
        driver_memory = None,
        driver_cores = None,
        executor_memory = None,
        executor_cores = None,
        num_executors = None,
        archives = None,
        queue = None,
        name = None,
        spark_conf = None,
        echo = True,
        check = True,
    ):
        self.client = LivyClient(url, auth, verify=verify)
        self.kind = kind
        self.proxy_user = proxy_user
        self.jars = jars
        self.py_files = py_files
        self.files = files
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors
        self.archives = archives
        self.queue = queue
        self.name = name
        self.spark_conf = spark_conf
        self.echo = echo
        self.check = check
        self.session_id = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def start(self):
        """Create the remote Spark session and wait for it to be ready."""

        session = self.client.create_session(
            self.kind,
            self.proxy_user,
            self.jars,
            self.py_files,
            self.files,
            self.driver_memory,
            self.driver_cores,
            self.executor_memory,
            self.executor_cores,
            self.num_executors,
            self.archives,
            self.queue,
            self.name,
            self.spark_conf,
        )
        self.session_id = session.session_id

        intervals = polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)
        while self.state in SESSION_STATE_NOT_READY:
            time.sleep(next(intervals))

    @property
    def state(self):
        """The state of the managed Spark session."""
        if self.session_id is None:
            raise ValueError("session not yet started")
        session = self.client.get_session(self.session_id)
        if session is None:
            raise ValueError("session not found - it may have been shut down")
        return session.state

    def close(self):
        """Kill the managed Spark session."""
        if self.session_id is not None:
            self.client.delete_session(self.session_id)
        self.client.close()

    def run(self, code):
        """Run some code in the managed Spark session.

        :param code: The code to run.
        """
        output = self._execute(code)
        if self.echo and output.text:
            print(output.text)
        if self.check:
            output.raise_for_status()
        return output

    def read(self, dataframe_name):
        """Evaluate and retrieve a Spark dataframe in the managed session.

        :param dataframe_name: The name of the Spark dataframe to read.
        """
        code = serialise_dataframe_code(dataframe_name, self.kind)
        output = self._execute(code)
        output.raise_for_status()
        if output.text is None:
            raise RuntimeError("statement had no text output")
        return deserialise_dataframe(output.text)

    def read_sql(self, code):
        """Evaluate a Spark SQL satatement and retrieve the result.

        :param code: The Spark SQL statement to evaluate.
        """
        if self.kind != SessionKind.SQL:
            raise ValueError("not a SQL session")
        output = self._execute(code)
        output.raise_for_status()
        if output.json is None:
            raise RuntimeError("statement had no JSON output")
        return dataframe_from_json_output(output.json)

    def _execute(self, code):
        if self.session_id is None:
            raise ValueError("session not yet started")

        statement = self.client.create_statement(self.session_id, code)

        intervals = polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)

        def waiting_for_output(statement):
            not_finished = statement.state in {
                StatementState.WAITING,
                StatementState.RUNNING,
            }
            available = statement.state == StatementState.AVAILABLE
            return not_finished or (available and statement.output is None)

        while waiting_for_output(statement):
            time.sleep(next(intervals))
            statement = self.client.get_statement(
                statement.session_id, statement.statement_id
            )

        if statement.output is None:
            raise RuntimeError("statement had no output")

        return statement.output
