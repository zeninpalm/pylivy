import logging

import requests

from livy.models import (
    Version,
    Session,
    SessionKind,
    Statement,
    StatementKind,
    Batch,
    BatchLog,
)


LOGGER = logging.getLogger(__name__)


VALID_LEGACY_SESSION_KINDS = {
    SessionKind.SPARK,
    SessionKind.PYSPARK,
    SessionKind.PYSPARK3,
    SessionKind.SPARKR,
}
VALID_SESSION_KINDS = {
    SessionKind.SPARK,
    SessionKind.PYSPARK,
    SessionKind.SPARKR,
    SessionKind.SQL,
    SessionKind.SHARED,
}


class JsonClient:
    """A wrapper for a requests session for JSON formatted requests.

    This client handles appending endpoints on to a common hostname,
    deserialising the response as JSON and raising an exception when an error
    HTTP code is received.
    """

    def __init__(
        self, url, auth = None, verify = True
    ):
        self.url = url
        self.session = requests.Session()
        if auth is not None:
            self.session.auth = auth
        self.session.verify = verify

    def close(self):
        self.session.close()

    def get(self, endpoint = "", params = None):
        return self._request("GET", endpoint)

    def post(self, endpoint, data = None):
        return self._request("POST", endpoint, data)

    def delete(self, endpoint = ""):
        return self._request("DELETE", endpoint)

    def _request(
        self,
        method,
        endpoint,
        data = None,
        params = None,
    ):
        url = self.url.rstrip("/") + endpoint
        response = self.session.request(method, url, json=data, params=params)
        response.raise_for_status()
        return response.json()


class LivyClient:
    def __init__(
        self, url, auth = None, verify = True
    ):
        self._client = JsonClient(url, auth, verify)
        self._server_version_cache = None

    def close(self):
        """Close the underlying requests session."""
        self._client.close()

    def server_version(self):
        """Get the version of Livy running on the server."""
        if self._server_version_cache is None:
            data = self._client.get("/version")
            self._server_version_cache = Version(data["version"])
        return self._server_version_cache

    def legacy_server(self):
        """Determine if the server is running a legacy version.

        Legacy versions support different session kinds than newer versions of
        Livy.
        """
        version = self.server_version()
        return version < Version("0.5.0-incubating")

    def list_sessions(self):
        """List all the active sessions in Livy."""
        data = self._client.get("/sessions")
        return [Session.from_json(item) for item in data["sessions"]]

    def create_session(
        self,
        kind,
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
    ):
        """Create a new session in Livy.

        The py_files, files, jars and archives arguments are lists of URLs,
        e.g. ["s3://bucket/object", "hdfs://path/to/file", ...] and must be
        reachable by the Spark driver process.  If the provided URL has no
        scheme, it's considered to be relative to the default file system
        configured in the Livy server.

        URLs in the py_files argument are copied to a temporary staging area
        and inserted into Python's sys.path ahead of the standard library
        paths.  This allows you to import .py, .zip and .egg files in Python.

        URLs for jars, py_files, files and archives arguments are all copied
        to the same working directory on the Spark cluster.

        The driver_memory and executor_memory arguments have the same format
        as JVM memory strings with a size unit suffix ("k", "m", "g" or "t")
        (e.g. 512m, 2g).

        See https://spark.apache.org/docs/latest/configuration.html for more
        information on Spark configuration properties.

        :param kind: The kind of session to create.
        :param proxy_user: User to impersonate when starting the session.
        :param jars: URLs of jars to be used in this session.
        :param py_files: URLs of Python files to be used in this session.
        :param files: URLs of files to be used in this session.
        :param driver_memory: Amount of memory to use for the driver process
            (e.g. '512m').
        :param driver_cores: Number of cores to use for the driver process.
        :param executor_memory: Amount of memory to use per executor process
            (e.g. '512m').
        :param executor_cores: Number of cores to use for each executor.
        :param num_executors: Number of executors to launch for this session.
        :param archives: URLs of archives to be used in this session.
        :param queue: The name of the YARN queue to which submitted.
        :param name: The name of this session.
        :param spark_conf: Spark configuration properties.
        """
        if self.legacy_server():
            valid_kinds = VALID_LEGACY_SESSION_KINDS
        else:
            valid_kinds = VALID_SESSION_KINDS

        if kind not in valid_kinds:
            raise ValueError(
                "%s is not a valid session kind for a Livy server of " % kind +
                "this version (should be one of %s)" % valid_kinds
            )

        body = {"kind": kind.value}
        if proxy_user is not None:
            body["proxyUser"] = proxy_user
        if jars is not None:
            body["jars"] = jars
        if py_files is not None:
            body["pyFiles"] = py_files
        if files is not None:
            body["files"] = files
        if driver_memory is not None:
            body["driverMemory"] = driver_memory
        if driver_cores is not None:
            body["driverCores"] = driver_cores
        if executor_memory is not None:
            body["executorMemory"] = executor_memory
        if executor_cores is not None:
            body["executorCores"] = executor_cores
        if num_executors is not None:
            body["numExecutors"] = num_executors
        if archives is not None:
            body["archives"] = archives
        if queue is not None:
            body["queue"] = queue
        if name is not None:
            body["name"] = name
        if spark_conf is not None:
            body["conf"] = spark_conf

        data = self._client.post("/sessions", data=body)
        return Session.from_json(data)

    def get_session(self, session_id):
        """Get information about a session.

        :param session_id: The ID of the session.
        """
        try:
            data = self._client.get("/sessions/%s" % session_id)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        return Session.from_json(data)

    def delete_session(self, session_id):
        """Kill a session.

        :param session_id: The ID of the session.
        """
        self._client.delete("/sessions/%s" % session_id)

    def list_statements(self, session_id):
        """Get all the statements in a session.

        :param session_id: The ID of the session.
        """
        response = self._client.get("/sessions/%s/statements" % session_id)
        return [
            Statement.from_json(session_id, data)
            for data in response["statements"]
        ]

    def create_statement(
        self, session_id, code, kind = None
    ):
        """Run a statement in a session.

        :param session_id: The ID of the session.
        :param code: The code to execute.
        :param kind: The kind of code to execute.
        """

        data = {"code": code}

        if kind is not None:
            if self.legacy_server():
                LOGGER.warning("statement kind ignored on Livy<0.5.0")
            data["kind"] = kind.value

        response = self._client.post(
            "/sessions/%s/statements" % session_id, data=data
        )
        return Statement.from_json(session_id, response)

    def get_statement(self, session_id, statement_id):
        """Get information about a statement in a session.

        :param session_id: The ID of the session.
        :param statement_id: The ID of the statement.
        """
        response = self._client.get(
            "/sessions/%s/statements/%s" % (session_id, statement_id)
        )
        return Statement.from_json(session_id, response)

    def create_batch(
        self,
        file,
        class_name = None,
        args = None,
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
    ):
        body = {"file": file}
        if class_name is not None:
            body["className"] = class_name
        if args is not None:
            body["args"] = args
        if proxy_user is not None:
            body["proxyUser"] = proxy_user
        if jars is not None:
            body["jars"] = jars
        if py_files is not None:
            body["pyFiles"] = py_files
        if files is not None:
            body["files"] = files
        if driver_memory is not None:
            body["driverMemory"] = driver_memory
        if driver_cores is not None:
            body["driverCores"] = driver_cores
        if executor_memory is not None:
            body["executorMemory"] = executor_memory
        if executor_cores is not None:
            body["executorCores"] = executor_cores
        if num_executors is not None:
            body["numExecutors"] = num_executors
        if archives is not None:
            body["archives"] = archives
        if queue is not None:
            body["queue"] = queue
        if name is not None:
            body["name"] = name
        if spark_conf is not None:
            body["conf"] = spark_conf

        data = self._client.post("/batches", data=body)
        return Batch.from_json(data)

    def delete_batch(self, batch_id):
        """Kill a batch session.

        :param batch_id: The ID of the session.
        """
        self._client.delete("/batches/%s" % batch_id)

    def get_batch(self, batch_id):
        """Get information about a batch.

        :param batch_id: The ID of the batch.
        """
        try:
            data = self._client.get("/batches/%s" % batch_id)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        return Batch.from_json(data)

    def get_batch_log(
        self, batch_id, from_ = None, size = None
    ):
        """Get logs for a batch.

        :param batch_id: The ID of the batch.
        :param from_: The line number to start getting logs from.
        :param size: The number of lines of logs to get.
        """
        params = {}
        if from_ is not None:
            params["from"] = from_
        if size is not None:
            params["size"] = size
        try:
            data = self._client.get("/batches/%s/log" % batch_id, params=params)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        return BatchLog.from_json(data)

    def list_batches(self):
        """List all the active batches in Livy."""
        response = self._client.get("/batches")
        return [Batch.from_json(data) for data in response["sessions"]]
