import time

from livy.client import LivyClient
from livy.models import SessionState, SESSION_STATE_FINISHED
from livy.utils import polling_intervals


class LivyBatch:
    def __init__(
        self,
        url,
        file,
        auth = None,
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
        self.client = LivyClient(url, auth)
        self.file = file
        self.class_name = class_name
        self.args = args
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
        self.batch_id = None

    def start(self):
        """Create the batch session.

        Unlike LivySession, this does not wait for the session to be ready.
        """
        batch = self.client.create_batch(
            self.file,
            self.class_name,
            self.args,
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
        self.batch_id = batch.batch_id

    def wait(self):
        """Wait for the batch session to finish."""

        intervals = polling_intervals([0.1, 0.5, 1.0, 3.0], 5.0)

        while True:
            state = self.state
            if state in SESSION_STATE_FINISHED:
                break
            time.sleep(next(intervals))

        return state

    @property
    def state(self):
        """The state of the managed Spark batch."""
        if self.batch_id is None:
            raise ValueError("batch session not yet started")
        batch = self.client.get_batch(self.batch_id)
        if batch is None:
            raise ValueError(
                "batch session not found - it may have been shut down"
            )
        return batch.state

    def log(self, from_ = None, size = None):
        """Get logs for this Spark batch.

        :param from_: The line number to start getting logs from.
        :param size: The number of lines of logs to get.
        """
        if self.batch_id is None:
            raise ValueError("batch session not yet started")
        log = self.client.get_batch_log(self.batch_id, from_, size)
        if log is None:
            raise ValueError(
                "batch session not found - it may have been shut down"
            )
        return log.lines

    def kill(self):
        """Kill the managed Spark batch session."""
        if self.batch_id is not None:
            self.client.delete_batch(self.batch_id)
        self.client.close()
