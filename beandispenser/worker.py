from logging import Logger
import signal
import beanstalkc
from commanding import Command, TimeOut, FailedJob
import sys
import time


class QueueServerConnection(Logger, object):

    _continue = True;

    def __init__(self, host, port, tube):
        self.host = host
        self.port = port
        self.tube = tube

    def stop(self):
        self._continue = False

    def disconnect(self):
        self._beanstalk.close()

    def connect(self):
        try:
            self._beanstalk = beanstalkc.Connection(host=self.host, port=self.port)
            self._beanstalk.ignore('default')
            self._beanstalk.watch(self.tube)
            self.info("connected")
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
                job = self._beanstalk.reserve(2)
                if job:
                    return job
            except beanstalkc.SocketError:
                self.error("lost connection")
                self.connect()


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
                handler = self._fail_handlers['on_permanent_fail']
            elif e.returncode == self.EXIT_CODE_TEMPORARY_FAIL:
                handler = self._fail_handlers['on_tempory_fail']
            else:
                handler = self._fail_handlers['on_unknown_fail']

            getattr(job, str(handler))()

        except TimeOut:
            self.info("job {0} timed out", job.stats()['id'])
            handler = self._fail_handlers['on_timeout']
            getattr(job, handler)()



class Worker(Logger, object):
    """A worker connects to the beanstalkc server and waits for jobs to
    become available.
    """

    STATE_WAITING = 1
    STATE_EXECUTING = 2

    # This is set to true on graceful stop.
    _exit_on_next_job = False

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
        self._connection = QueueServerConnection(host=str(connection['host']),
                                                 port=connection['port'],
                                                 tube=tube)
        self._connection.connect()
        self.info("Start watching tube {0}", tube)
        self._command = command

    def watch(self):
        """Start watching a tube for incoming jobs"""

        while self._exit_on_next_job == False:

            self._state = self.STATE_WAITING
            job = self._connection.get_job()
            self._state = self.STATE_EXECUTING

            if self._exit_on_next_job == True:
                self.close()

            self.info("job {0} accepted", job.stats()['id'])

            try:
                time_left = job.stats()["time-left"]

                if time_left > 0:
                    command = Command(self._command, time_left, job.body)
                    command.run()

                    self.info("job {0} done", job.stats()['id'])

                    job.delete()
                else:
                    handler = self._fail_handlers['on_tempory_fail']
                    getattr(job, str(handler))()

            except beanstalkc.SocketError:
                self.error("lost connection")
                self._connction.connect()

            except Exception as e:
                self._error_handler.handle(e, job)            

        self.close()

    def stop(self, signum, frame):
        """Perform a graceful stop"""
        self._exit_on_next_job = True
        self._connection.stop()

    def close(self):
        sys.exit()