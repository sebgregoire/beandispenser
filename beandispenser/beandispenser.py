from logging import Logger
from config import Config
from worker import Worker
from yaml.scanner import ScannerError
import os
import signal

class ErrorActions(object):

    def __init__(self, error_codes):
        self.error_codes = error_codes

    def get_action(self, exception, actions):

        for error, action in actions.iteritems():
            if error in self.error_codes and self.error_codes[error] == exception.returncode:
                return action
        return 'bury'

class Forker(Logger, object):
    """The forker takes care of creating a fork for each worker.
    """

    def __init__(self, config_path):
        """Constructor

        param tubes: a list of tube configs, one per worker pool
        """
        self.read_config(config_path)
        # Ignore the SIGINT, SIGTERM and SIGQUIT. Let the workers do the quiting.
        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, signal.SIG_IGN)

    def read_config(self, path):
        try:
            self.config = Config()
            self.config.read(open(path).read())
        except ScannerError as yaml_error:
            print "\nYour config file has an error : '{}' :\n".format(yaml_error.problem)
            print '---'
            print yaml_error.problem_mark
            exit(2)

    def fork_all(self):
        """Create a fork for each worker. The number of workers per tube is
        specified in the tubes list passed to the constructor.
        """
        error_actions = ErrorActions(self.config['error_codes'])
        pids = []

        self.info('Parent process started with pid {}'.format(os.getpid()))

        for tube_config in self.config['tubes']:

            try: worker_count = tube_config['workers']
            except KeyError: worker_count = 1

            for i in range(worker_count):

                # fork the current process. The parent and the child both continue
                # from this point so we need to make sure that only the child
                # process adds workers to the pool.
                pid = os.fork()
                
                if pid == 0:
                    # child process
                    self.info('Child process started with pid {} on tube "{}"'.format(os.getpid(), tube_config['name']))
                    worker = Worker(os.getpid(), tube_config, self.config['connection'], error_actions)
                    worker.watch()

                    sys.exit()
                else:
                    pids.append(pid)

        for pid in pids:
            os.waitpid(pid, 0)
            self.info("Worker {} has exited.".format(pid))

def main():

    try:
        config_path = os.environ['BEANDISPENDER_CONFIG_FILE']
    except KeyError:
        print 'You need to specify the BEANDISPENDER_CONFIG_FILE environment variable.'
        exit(1)

    Forker(config_path).fork_all()

    # good manners
    print "\nbye"

if __name__ == "__main__":
    main()