from logging import Logger
import signal
import beanstalkc
from commanding import Command, TimeOut, FailedJob
import sys
import time

class Graceful(Exception):
    pass

class Job(Logger, beanstalkc.Job):
    
    _on_exit = 'delete'

    def execute_command(self, command, error_handler):
        try:
            stats = self.stats()

            if stats["time-left"] > 0:
                command = Command(command, stats["time-left"], self.body)
                command.run()

                self.info("job {0} done", stats['id'])
            else:
                error_handler.handle(TimeOut(), self)

        except Exception as e:
            error_handler.handle(e, self)

    def on_exit(self, action):
        self._on_exit = action

    def __enter__(self):
        self.info("job {0} accepted", self.stats()['id'])
        return self

    def __exit__(self, type, value, traceback):
        if self._on_exit == 'release':
            self.release(None, 60)
        else:
            getattr(self, self._on_exit)()


class BeanstalkConnection(beanstalkc.Connection, Logger, object):

    _continue = True;

    def __init__(self, host, port, tube):
        self.tube = tube
        super(BeanstalkConnection, self).__init__(host, port)

    def _interact_job(self, command, expected_ok, expected_err, reserved=True):
        jid, size = self._interact(command, expected_ok, expected_err)
        body = self._read_body(int(size))
        return Job(self, int(jid), body, reserved)

    def stop(self):
        self._continue = False

    def connect(self):
        try:
            super(BeanstalkConnection, self).connect()
            self.ignore('default')
            self.watch(self.tube)
        except beanstalkc.SocketError:
            if self._continue:
                self.error("Failed to connect. Retrying in 5 seconds")
                time.sleep(5)
                self.connect()

    def get_job(self):
        """Reserve a job from the tube and set appropriate worker state.
        Only block the socket for 2 seconds so we can catch graceful stops  """

        while self._continue:
            try:
                job = self.reserve(2)
                if job:
                    return job
            except beanstalkc.SocketError:
                self.error("lost connection")
                self.connect()
        raise Graceful;


class ErrorHandler(Logger, object):

    # exit codes from commands are interpreted and used to decide what to do with
    # commands when they succeed or fail. Example: a successfully completed job
    # needs to be deleted. A command that failed permanently (example: a job that
    # needed to work on a resource that no longer exists) needs to be treated
    # differently than a job that fails temporarily (example: a job that depends on
    # an external API that is temporarily unavailable).
    EXIT_CODE_SUCCESS= 0
    EXIT_CODE_PERMANENT_FAIL = 1
    EXIT_CODE_TEMPORARY_FAIL = 2

    # keep track of the current job for logging purposes
    _current_job = None

    # This specifies the behaviour when a command exits with anything else than
    # an exit code of 0. Beanstalkd only offers default behaviour for _on_timeout
    # and assumes failed jobs are buried by the connected worker. Default behaviour
    # for each fail reason is to bury the job, but this an be changed in the config
    # for either 'bury', 'delete', or 'release'
    _fail_handlers = {
        "on_permanent_fail" : None,
        "on_temporary_fail" : None,
        "on_unknown_fail" : None,
        "on_timeout" : None
    }

    def __init__(self, on_permanent_fail='bury', on_temporary_fail='bury',
                                on_unknown_fail='bury', on_timeout='release'):
        for handler in self._fail_handlers:
            self._fail_handlers[handler] = locals()[handler]

    def handle(self, e, job):
        try:
            raise e
        except FailedJob as e:
            self.info("job {0} failed with return code {1} and message '{2}'",
                                job.stats()['id'], e.returncode, e.message)
            if e.returncode == self.EXIT_CODE_PERMANENT_FAIL:
                job.on_exit(str(self._fail_handlers['on_permanent_fail']))
            elif e.returncode == self.EXIT_CODE_TEMPORARY_FAIL:
                job.on_exit(str(self._fail_handlers['on_temporary_fail']))
            else:
                job.on_exit(str(self._fail_handlers['on_unknown_fail']))
        except TimeOut:
            self.info("job {0} timed out", job.stats()['id'])
            job.on_exit(str(self._fail_handlers['on_timeout']))



class Worker(Logger, object):
    """A worker connects to the beanstalkc server and waits for jobs to
    become available.
    """
    def __init__(self, pid, tube, command, connection, on_permanent_fail='bury',
                                on_temporary_fail='bury', on_unknown_fail='bury',
                                on_timeout='release'):
        """Constructor.

        :param tube: an dict containing information on the tube to watch
        :param pid:  the PID of the worker fork
        """

        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, self.stop)

        self._error_handler = ErrorHandler()
        self._connection = BeanstalkConnection(host=str(connection['host']),
                                                 port=connection['port'],
                                                 tube=tube)
        self._connection.connect()
        self.info("Start watching tube {0}", tube)
        self._command = command

    def watch(self):
        """Start watching a tube for incoming jobs"""
        try:
            while True:
                with self._connection.get_job() as job:
                    job.execute_command(self._command, self._error_handler)
        except Graceful:
            self.close()

    def stop(self, signum, frame):
        """Perform a graceful stop"""
        self._connection.stop()

    def close(self):
        sys.exit()