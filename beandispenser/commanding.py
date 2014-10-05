import shlex
import threading
import traceback
import subprocess
import signal

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

            self.process.terminate()

            if thread.is_alive():
                self.process.kill()

            raise TimeOut

        if self.returncode != 0:
            raise FailedJob(self.error, self.returncode)

        return self.output
