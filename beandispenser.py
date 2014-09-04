"""
Heavily inspired by http://jacobian.org/writing/python-is-unix/
"""
import threading
import traceback
import os
import sys
import beanstalkc
import time
import subprocess
import shlex

beanstalkd_host = "localhost"
beanstalkd_port = 11300

class TimeOut(Exception):
    pass

class FailedJob(Exception):

    def __init__(self, message, returncode):
        Exception.__init__(self, message)
        self.returncode = returncode


class Worker(object):

    def __init__(self, tube, pid):

        self._beanstalk = beanstalkc.Connection(host=beanstalkd_host, port=beanstalkd_port)
        self._tube = tube
        self._pid = pid

    def watch(self):

        try:
            self._beanstalk.watch(self._tube["name"])
            self._beanstalk.ignore('default')

            while True:
                job = self._beanstalk.reserve()

                try:
                    command = Command(self._tube["command"], self._tube["ttr"], job.body)
                    command.run()

                    job.delete()
                
                except FailedJob as e:
                    print "failed job in pid %d" % self._pid

                    job.bury()

                except TimeOut:
                    print "timeout in pid %d" % self._pid
                    job.bury()

        except KeyboardInterrupt:
            sys.exit()

             
class Command(object):
 
    def __init__(self, command, timeout, input):
        self.command = shlex.split(command)
        self.timeout = timeout
        self.input = input
 
    def target(self):
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

        # thread
        thread = threading.Thread(target=self.target)
        thread.start()

        # join the new thread (blocking). This unblocks after <timeout>
        thread.join(self.timeout)

        # if the thread is still alive after <timeout>, it means the join
        # timed out. Terminate the process and exit.
        if thread.is_alive():
            self.process.terminate()
            # renew the join so we wait till the terminated process exits
            thread.join()

            raise TimeOut

        if self.returncode != 0:
            raise FailedJob(self.error, self.returncode)

        return self.output


class WorkerPool(object):

    _workers = []

    def __init__(self, tube):

        self._tube = tube

    def add_new_worker(self, pid):
        self._workers.append(Worker(self._tube, pid))

    def watch(self):

        for worker in self._workers:
            worker.watch()


class Forker(object):

    _pools = []

    def __init__(self, tubes):
        self._tubes = tubes


    def fork_all(self):

        for tube in self._tubes:

            self.fork_for_tube(tube)


    def fork_for_tube(self, tube):

        pool = WorkerPool(tube)
        self._pools.append(pool)

        # Fork you some child processes. In the parent, the call to
        # fork returns immediately with the pid of the child process;
        # fork never returns in the child because we exit at the end
        # of the block.

        for i in range(tube["workers"]):

            pid = os.fork()
            
            # os.fork() returns 0 in the child process and the child's
            # process id in the parent. So if pid == 0 then we're in
            # the child process.
            if pid == 0:

                # now we're in the child process; trap (Ctrl-C)
                # interrupts by catching KeyboardInterrupt) and exit
                # immediately instead of dumping stack to stderr.
                childpid = os.getpid()

                pool.add_new_worker(childpid)
                pool.watch()


if __name__ == "__main__":

    tubes = [
        {
            "name" : "foo",
            "workers" : 3,
            "ttr" : 1,
            "command" : "sleep"
        },
        {
            "name" : "bar",
            "workers" : 4,
            "ttr" : 1,
            "command" : "sleep",
            "bury_on_timeout" : True,
            "bury_on_fail" : True
        }
    ]


    try:
        forker = Forker(tubes)
        forker.fork_all()

        os.waitpid(-1, 0)
    except KeyboardInterrupt:
        print "\nbye"
        sys.exit()

