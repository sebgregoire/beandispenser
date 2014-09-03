"""
Heavily inspired by http://jacobian.org/writing/python-is-unix/
"""
 
import os
import sys
import beanstalkc
import time

beanstalkd_host = "localhost"
beanstalkd_port = 11300

class Worker(object):

    def __init__(self, tube, pid):

        self._beanstalk = beanstalkc.Connection(host=beanstalkd_host, port=beanstalkd_port)
        self._tube = tube
        self._pid = pid

    def watch(self):

        try:
            self._beanstalk.watch(self._tube)
            self._beanstalk.ignore('default')

            while True:
                job = self._beanstalk.reserve()
                job.delete()

        except KeyboardInterrupt:
            sys.exit()


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

        pool = WorkerPool(tube["name"])
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
            "workers" : 3
        },

        {
            "name" : "bar",
            "workers" : 4
        },
    ]


    try:
        forker = Forker(tubes)
        forker.fork_all()

        os.waitpid(-1, 0)
    except KeyboardInterrupt:
        print "\nbye"
        sys.exit()

