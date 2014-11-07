from logging import Logger
from config import Config
from worker import Worker
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
    _pids = []

    def __init__(self, config):
        """Constructor

        param tubes: a list of tube configs, one per worker pool
        """

        self.config = config

        # Ignore the SIGINT, SIGTERM and SIGQUIT. Let the workers do the quiting.
        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signum, signal.SIG_IGN)

    def fork_all(self):
        """Create a fork for each worker. The number of workers per tube is
        specified in the tubes list passed to the constructor.
        """

        self.info("Start forking")
        error_actions = ErrorActions(self.config['error_codes'])

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
                    worker = Worker(os.getpid(), tube_config, self.config['connection'], error_actions)
                    worker.watch()

                    sys.exit()
                else:
                    self._pids.append(pid)

        for pid in self._pids:
            os.waitpid(pid, 0)

def main():

    config = Config()
    config.read(open(os.environ['BEANDISPENDER_CONFIG_FILE']).read())
    Forker(config).fork_all()

    # good manners
    print "\nbye"

if __name__ == "__main__":
    main()
