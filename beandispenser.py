import threading
import traceback
import os
import sys
import beanstalkc
import time
import subprocess
import shlex

#todo: move this to configs
beanstalkd_host = "localhost"
beanstalkd_port = 11300

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


class Worker(object):
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

    def __init__(self, tube, pid):
        """Constructor.

        :param tube: an dict containing information on the tube to watch
        :param pid:  the PID of the worker fork
        """

        print "Worker %d is now watching tube %s" % (pid, tube['name'])

        self._beanstalk = beanstalkc.Connection(host=beanstalkd_host,
                                                port=beanstalkd_port)
        self._tube = tube
        self._pid = pid

        if 'on_fail' in tube and tube['on_fail'] in ['bury', 'release']:
            self._on_fail = tube['on_fail']
        if 'on_timeout' in tube and tube['on_timeout'] in ['bury', 'release']:
            self._on_timeout = tube['on_timeout']

    def watch(self):
        """Start watching a tube for incoming jobs"""

        try:
            self._beanstalk.watch(self._tube["name"])
            self._beanstalk.ignore('default')

            while True:
                # graceful stop
                if self._exit_on_next_job == True:
                    sys.exit()

                job = self._reserve_job()
                print "job %d accepted by worker %d" % (job.stats()['id'], self._pid)

                try:
                    time_left = job.stats()["time-left"]

                    if time_left > 0:
                        command = Command(self._tube["command"], time_left, job.body)
                        command.run()

                        print "job %d done" % job.stats()['id']

                        job.delete()
                    else:
                        self._bury_or_release(job, self._on_timeout)
                
                except FailedJob as e:
                    self._bury_or_release(job, self._on_fail)

                except TimeOut:
                    self._bury_or_release(job, self._on_timeout)

        except KeyboardInterrupt:
            print "graceful"
            self._graceful_stop()

    def _reserve_job(self):
        """Reserve a job from the tube and set appropriate worker state"""
        self._state = self.STATE_WAITING
        self._current_job = self._beanstalk.reserve()
        self._state = self.STATE_EXECUTING        
        return self._current_job

    def _bury_or_release(self, job, action):
        """Bury or release a job

        param job:    the job to bury or release
        param action: the action to perform on the job
        """
        if action == 'release':
            print "Worker %d releases a job" % self._pid
            job.release(job.stats()['pri'], 60)
        else:
            print "Worker %d buries a job" % self._pid
            job.bury()

    def _graceful_stop(self):
        """Perform a graceful stop"""
        if self._state == self.STATE_WAITING:
            print "Worker %d exiting immediately" % self._pid
            sys.exit()
        else:
            print "Worker %d finishing job %d before exiting" % (self._pid, self._current_job.stats()['id'])
            self._exit_on_next_job = True


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
 
    def target(self):
        """The target of the command thread. IE the actual execution of
        the command.
        """
        try:
            self.process = subprocess.Popen(self.command,
                                    stdin  = subprocess.PIPE,
                                    stdout = subprocess.PIPE,
                                    stderr = subprocess.PIPE)
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

        if thread.is_alive():
            self.process.terminate()
            thread.join()

            raise TimeOut

        if self.returncode != 0:
            raise FailedJob(self.error, self.returncode)

        return self.output


class WorkerPool(object):
    """A pool of workers all listening to the same tube
    """
    _workers = []

    def __init__(self, tube):
        """Constructor

        param tube: information abou the tube that workers in this pool will watch
        """
        self._tube = tube

    def add_new_worker(self, pid):
        """Add a new worker to the pool. Each worker runs in its own fork.

        param pid: the PID of the worker fork
        """
        self._workers.append(Worker(self._tube, pid))

    def watch(self):

        """Tell all workers in the pool to start watching their assigned tube
        """
        for worker in self._workers:
            worker.watch()


class Forker(object):
    """The forker takes care of creating a fork for each worker.
    """
    _worker_pools = []

    def __init__(self, tubes):
        """Constructor

        param tubes: a list of tube configs, one per worker pool
        """
        self._tubes = tubes


    def fork_all(self):
        """Create a fork for each worker. The number of workers per tube is
        specified in the tubes list passed to the constructor.
        """
        for tube in self._tubes:

            pool = WorkerPool(tube)
            self._worker_pools.append(pool)

            for i in range(tube["workers"]):

                # fork the current process. The parent and the child both continue
                # from this point so we need to make sure that only the child
                # process adds workers to the pool.
                pid = os.fork()
                
                # If pid == 0 then we're in the child process.
                if pid == 0:

                    fork_pid = os.getpid()

                    pool.add_new_worker(fork_pid)
                    pool.watch()

                    # once pool.watch() unblocks it means the worker in this fork
                    # has stopped watching (hopefully because of a graceful stop)
                    # so we need to exit the fork.
                    sys.exit()


if __name__ == "__main__":

    tubes = [
        {
            "name" : "foofoo",
            "workers" : 3,
            "command" : "sleep 20",
            "on_timeout" : "bury",
            "on_fail" : "bury"

        },
        {
            "name" : "barbar",
            "workers" : 4,
            "command" : "sleep 60",
            "on_timeout" : "bury",
            "on_fail" : "bury"
        }
    ]


try:
    forker = Forker(tubes)
    forker.fork_all()

    os.waitpid(-1, 0)
except KeyboardInterrupt:
    os.waitpid(-1, 0)

    print "\nbye"
    sys.exit()

