from logging import Logger
import signal
import beanstalkc
from commanding import Command, TimeOut, FailedJob
import sys
import time
import socket
import yaml

class Graceful(Exception):
    pass

class BeanstalkConnection(beanstalkc.Connection, Logger, object):

    _continue = True;

    def __init__(self, host, port, tube):
        self.tube, self.host, self.port = tube, host, port
        self._connect_timeout = socket.getdefaulttimeout()
        self._parse_yaml = yaml.load

    def stop(self):
        self._continue = False

    def connect(self):
        try:
            super(BeanstalkConnection, self).connect()
            self.ignore('default')
            self.watch(self.tube)
            self.info("Connected.")
        except beanstalkc.SocketError:
            if self._continue:
                self.error("Failed to connect. Retrying in 2 seconds.")
                time.sleep(2)
                if self._continue:
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
        raise Graceful


class Worker(Logger, object):
    """A worker connects to the beanstalkc server and waits for jobs to
    become available.
    """
    def __init__(self, pid, tube_config, connection_config, error_actions):
        """Constructor.

        :param tube: an dict containing information on the tube to watch
        :param pid:  the PID of the worker fork
        """

        self._connection = BeanstalkConnection(host=str(connection_config['host']),
                                                 port=connection_config['port'],
                                                 tube=tube_config['name'])

        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, self.stop)

        self._connection.connect()
        self._command = tube_config['command']
        self.error_actions = error_actions
        self.error_handling = tube_config['error_handling'] if 'error_handling' in tube_config else {}
        self.tube_name = tube_config['name']

    def watch(self):
        """Start watching a tube for incoming jobs"""
        while True:
            try:
                job = self._connection.get_job()
                stats = job.stats()

                if stats["time-left"] > 0:
                    command = Command(self._command, stats["time-left"], job.body)
                    command.run()
                else:
                    raise TimeOut()
            except Graceful:
                sys.exit()
            except beanstalkc.SocketError:
                self._connection.connect()
            except FailedJob as error:
                self.handle_failed_command(error, job)
            except TimeOut:
                self.handle_timeout(job)

    def handle_failed_command(self, error, job):
        """Handle a failed command by performing the configured action on the job"""
        action = self.error_actions.get_action(error, self.error_handling)
        if action in ['bury', 'delete', 'release']:
            if action == 'release' : job.release(delay=60)
            else: getattr(job, action)()
        else:
            self.error('Invalid error handler specified for tube {} : {}. Burying instead.'.format(self.tube_name, action))
            job.bury()

    def handle_timeout(self, job):
        """Handle the case where a job took longer that its allowed ttr"""
        try:
            if self.error_handling['timeout'] in ['delete', 'release', 'bury']:
                if self.error_handling['timeout'] == 'release' : job.release(delay=60)
                else: getattr(job, self.error_handling['timeout'])()
            else:
                job.bury()
        except KeyError:
            job.bury()

    def stop(self, signum, frame):
        """Perform a graceful stop"""
        self._connection.stop()
