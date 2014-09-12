import threading
import traceback
import os
import sys
import beanstalkc
import time
import subprocess
import shlex
import signal
from ConfigParser import ConfigParser
import syslog

config = ConfigParser()
config.read(os.environ['BEANDISPENDER_CONFIG_FILE'])

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


class Worker(Logger, object):
    """A worker connects to the beanstalkc server and waits for jobs to
    become available.
    """

    STATE_WAITING = 1
    STATE_EXECUTING = 2

    _current_job = None
    _on_fail = 'bury'
    _on_timeout = 'release'
    _state = None
    _exit_on_next_job = False

    def __init__(self, pool, pid):
        """Constructor.

        :param tube: an dict containing information on the tube to watch
        :param pid:  the PID of the worker fork
        """

        super(Worker, self).__init__(pool['tube'])

        self.info("Start watching tube {0}", pool['tube'])

        self._beanstalk = beanstalkc.Connection(
            host=config.get('connection', 'host'),
            port=int(config.get('connection', 'port')))

        self._pool = pool
        self._pid = pid

        if 'on_fail' in pool and pool['on_fail'] in ['bury', 'release']:
            self._on_fail = pool['on_fail']
        if 'on_timeout' in pool and pool['on_timeout'] in ['bury', 'release']:
            self._on_timeout = pool['on_timeout']

        # When the process is asked politely to stop, stop gracefully
        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, self.stop)

    def watch(self):
        """Start watching a tube for incoming jobs"""

        self._beanstalk.watch(self._pool["tube"])
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
                    command = Command(self._pool["command"], time_left, job.body)
                    command.run()

                    self.info("job {0} done", job.stats()['id'])

                    job.delete()
                else:
                    self._bury_or_release(job, self._on_timeout)
            
            except FailedJob as e:
                self.info("job {0} failed with return code {1} and message '{2}'",
                                    job.stats()['id'], e.returncode, e.message)
                self._bury_or_release(job, self._on_fail)

            except TimeOut:
                self.info("job {0} timed out", job.stats()['id'])
                self._bury_or_release(job, self._on_timeout)

    def _reserve_job(self):
        """Reserve a job from the tube and set appropriate worker state.
        Only block the socket for 2 seconds so we can catch graceful stops  """
        self._state = self.STATE_WAITING
        self._current_job = self._beanstalk.reserve(2)
        self._state = self.STATE_EXECUTING        
        return self._current_job

    def _bury_or_release(self, job, action):
        """Bury or release a job

        param job:    the job to bury or release
        param action: the action to perform on the job
        """
        if action == 'release':
            job.release(job.stats()['pri'], 60)
        else:
            job.bury()

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
        self.command = shlex.split(command)
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

    def __init__(self):
        """Constructor

        param tubes: a list of tube configs, one per worker pool
        """

        super(Forker, self).__init__("main")

        # Ignore the SIGINT, SIGTERM and SIGQUIT. Let the workers do the quiting.
        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, signal.SIG_IGN)

        for section, tube in [(s, s[5:]) for s in config.sections() if s[0:5] == 'tube:']:

            pool = dict([(key, config.get(section, key)) for key in [
                'workers',
                'command',
                'on_timeout',
                'on_fail'
            ] if config.has_option(section, key)])

            pool.update({"tube" : tube})
            self._pools.append(pool)

    def fork_all(self):
        """Create a fork for each worker. The number of workers per tube is
        specified in the tubes list passed to the constructor.
        """

        self.info("Start forking")

        for pool in self._pools:

            for i in range(int(pool["workers"])):

                # fork the current process. The parent and the child both continue
                # from this point so we need to make sure that only the child
                # process adds workers to the pool.
                pid = os.fork()
                
                if pid == 0:
                    # child process
                    worker = Worker(pool, os.getpid())
                    worker.watch()

                    sys.exit()
                else:
                    self._pids.append(pid)

        for pid in self._pids:
            os.waitpid(pid, 0)

def main():
    Forker().fork_all()

    # good manners
    print "\nbye"

if __name__ == "__main__":
    main()
