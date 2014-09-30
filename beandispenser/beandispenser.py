import threading
import traceback
import os
import sys
import beanstalkc
import time
import subprocess
import shlex
import signal
from ConfigParser import ConfigParser, NoOptionError
import syslog

class ConfigValueError(Exception):
    pass

class ConfigValue(object):

    def __init__(self, section, key, value):

        if key[:3] == 'on_' and value not in ['delete', 'bury', 'release']:
            raise ConfigValueError('[{0} {1}] value must be on of delete, bury, release'.format(section, key))

        self._section = section
        self._key = key
        self._value = value

    def __int__(self):
        if self._key in ['workers', 'port']:
            return int(self._value)
        raise ConfigValueTypeError()

    def __str__(self):
        return self._value

    def __repr__(self):
        return self._value        

    def __unicode__(self):
        return self._value


class Config(ConfigParser):
    """Just a simple way of getting None back if the config doesn't exist
    """
    def get(self, section, key, default=None):
        try:
            value = ConfigParser.get(self, section, key)
            return ConfigValue(section, key, value)
        except NoOptionError:
            return None


class TimeOut(Exception):
    """An exception for a job that times out before the TTR
    """
    pass


class FailedJob(Exception):
    """An exception for a job that returns a status code other than 0
    """
    def __init__(self, message, returncode):
        Exception.__init__(self, message)
        self.returncode = returncode


class Logger(object):

    def __init__(self, name):
        self._name = name

    def info(self, msg, *args):
        syslog.openlog(self._name, syslog.LOG_PID, syslog.LOG_USER)
        syslog.syslog(msg.format(*args))
        syslog.closelog()

    def error(self, msg, *args):
        syslog.openlog(self._name, syslog.LOG_PID, syslog.LOG_USER)
        syslog.syslog('Error : ' + msg.format(*args))
        syslog.closelog()


class Worker(Logger, object):
    """A worker connects to the beanstalkc server and waits for jobs to
    become available.
    """

    STATE_WAITING = 1
    STATE_EXECUTING = 2

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

    # the current state of this worker. Either STATE_WAITING or STATE_EXECUTING
    _state = None

    # This is set to true on graceful stop.
    _exit_on_next_job = False

    def __init__(self, pid, tube, command, connection, on_permanent_fail='bury',
                                on_temporary_fail='bury', on_unknown_fail='bury',
                                on_timeout='release'):
        """Constructor.

        :param tube: an dict containing information on the tube to watch
        :param pid:  the PID of the worker fork
        """

        super(Worker, self).__init__(tube)

        self.connect(connection)
        self.info("Start watching tube {0}", tube)

        self._pid = pid
        self._tube = tube
        self._command = command

        for handler in self._fail_handlers:
            self._fail_handlers[handler] = locals()[handler]

        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, self.stop)

    def connect(self, connection):

        try:
            self._beanstalk = beanstalkc.Connection(
                host=str(connection['host']),
                port=connection['port'])
        except beanstalkc.SocketError:
            self.error("Failed to connect. Retrying in 5 seconds")
            time.sleep(5)
            self.connect()

    def watch(self):
        """Start watching a tube for incoming jobs"""

        self._beanstalk.watch(self._tube)
        self._beanstalk.ignore('default')

        while True:
            # graceful stop
            if self._exit_on_next_job == True:
                self.info("Graceful stop")
                self.close()

            job = self._reserve_job()
            if not job:
                continue

            self.info("job {0} accepted", job.stats()['id'])

            try:
                time_left = job.stats()["time-left"]

                if time_left > 0:
                    command = Command(self._command, time_left, job.body)
                    command.run()

                    self.info("job {0} done", job.stats()['id'])

                    job.delete()
                else:
                    self._bury_or_release(job, self._on_timeout)
            
            except FailedJob as e:
                self.info("job {0} failed with return code {1} and message '{2}'",
                                    job.stats()['id'], e.returncode, e.message)

                if e.returncode == self.EXIT_CODE_PERMANENT_FAIL:
                    handler = self._fail_handlers['on_permanent_fail']
                elif e.returncode == self.EXIT_CODE_TEMPORARY_FAIL:
                    handler = self._fail_handlers['on_temporary_fail']
                else:
                    handler = self._fail_handlers['on_unknown_fail']

                getattr(job, str(handler))()

            except TimeOut:
                self.info("job {0} timed out", job.stats()['id'])
                handler = self._fail_handlers['on_timeout']
                getattr(job, handler)()

    def _reserve_job(self):
        """Reserve a job from the tube and set appropriate worker state.
        Only block the socket for 2 seconds so we can catch graceful stops  """
        try:
            self._state = self.STATE_WAITING
            self._current_job = self._beanstalk.reserve(2)
            self._state = self.STATE_EXECUTING        
            return self._current_job
        except beanstalkc.SocketError:
            self.connect()
            return self._reserve_job()

    def stop(self, signum, frame):
        """Perform a graceful stop"""
        if self._state == self.STATE_EXECUTING:
            self.info("Finishing job {0} before exiting", self._current_job.stats()['id'])
        self._exit_on_next_job = True

    def close(self):
        self._beanstalk.close()
        sys.exit()

class Command(object):
    """When a worker reserves a job, it executes a command and passes
    the job body to its stdin. Each command has a certain allowed time
    to run (TTR). If that time is exceeded, the process is terminated
    and a TimeOut exception is raised to notify the worker of the event.
    If the command has a return code other than 0, a FailedJob exception
    is raised.
    """ 

    def __init__(self, command, timeout, input):
        """Constructor

        param command: the command to run
        param timeout: the time allowed to run
        param input:   the content to pass to the command's stdin
        """ 
        self.command = shlex.split(str(command))
        self.timeout = timeout
        self.input = input
 
    def preexec_function(self):
        # Ignore the SIGINT, SIGTERM and SIGQUIT. Let the worker do the quiting.
        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, signal.SIG_IGN)

    def target(self):
        """The target of the command thread. IE the actual execution of
        the command.
        """
        try:
            self.process = subprocess.Popen(self.command,
                                    stdin  = subprocess.PIPE,
                                    stdout = subprocess.PIPE,
                                    stderr = subprocess.PIPE,
                                    preexec_fn = self.preexec_function)
            self.process.stdin.write(self.input)
            self.output, self.error = self.process.communicate()
            self.returncode = self.process.returncode

        except:
            self.error = traceback.format_exc()
            self.returncode = -1

    def run(self):
        """
        Start a thread that will run the command and join it for the number
        of seconds specified by timeout. If the thread is still alive after
        this time, it is terminated.
        """

        thread = threading.Thread(target=self.target)
        thread.start()

        # join the new thread (blocking). This unblocks after <timeout> or when
        # the thread is done.
        thread.join(self.timeout)

        # if the thread is still alive, it means it's taking longer than ttr
        if thread.is_alive():

            # give the process 2 seconds to finish properly
            self.process.terminate()

            # if it hasn't exited after 2 seconds, kill it.
            if thread.is_alive():
                self.process.kill()

            raise TimeOut

        if self.returncode != 0:
            raise FailedJob(self.error, self.returncode)

        return self.output

class Forker(Logger, object):
    """The forker takes care of creating a fork for each worker.
    """

    _pids = []
    _pools = []

    def __init__(self, config):
        """Constructor

        param tubes: a list of tube configs, one per worker pool
        """

        super(Forker, self).__init__("main")

        # Ignore the SIGINT, SIGTERM and SIGQUIT. Let the workers do the quiting.
        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, signal.SIG_IGN)

        for section, tube in [(s, s[5:]) for s in config.sections() if s[0:5] == 'tube:']:

            pool = {
                "worker_count" : config.get(section, "workers", 1),
            }

            kwargs = {
                "tube" : tube,
                "command" : config.get(section, "command"),
                "on_permanent_fail" : config.get(section, "on_permanent_fail"),
                "on_temporary_fail" : config.get(section, "on_temporary_fail"),
                "on_unknown_fail" : config.get(section, "on_unknown_fail"),
                "on_timeout" : config.get(section, "on_timeout"),
                "connection" : {
                    "host" : config.get('connection', 'host'),
                    "port" : config.get('connection', 'port')
                }
            }

            pool['kwargs'] = {k:v for (k, v) in kwargs.iteritems() if v}

            self._pools.append(pool)

    def fork_all(self):
        """Create a fork for each worker. The number of workers per tube is
        specified in the tubes list passed to the constructor.
        """

        self.info("Start forking")

        for pool in self._pools:

            for i in range(pool["worker_count"]):

                # fork the current process. The parent and the child both continue
                # from this point so we need to make sure that only the child
                # process adds workers to the pool.
                pid = os.fork()
                
                if pid == 0:
                    # child process
                    worker = Worker(os.getpid(), **pool['kwargs'])
                    worker.watch()

                    sys.exit()
                else:
                    self._pids.append(pid)

        for pid in self._pids:
            os.waitpid(pid, 0)

def main():

    config = Config()
    config.read(os.environ['BEANDISPENDER_CONFIG_FILE'])

    Forker(config).fork_all()

    # good manners
    print "\nbye"

if __name__ == "__main__":
    main()
