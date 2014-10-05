from logging import Logger
from config import Config
from worker import Worker
import os
import signal
    
class Forker(Logger, object):
    """The forker takes care of creating a fork for each worker.
    """

    _pids = []
    _pools = []

    def __init__(self, config):
        """Constructor

        param tubes: a list of tube configs, one per worker pool
        """

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
