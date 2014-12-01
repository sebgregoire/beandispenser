import sys, os

class Logger(object):

    def info(self, msg, *args):
        sys.stdout.write("beandispenser (pid {}) info: {} \n".format(os.getpid(), msg.format(*args)))
        sys.stdout.flush()

    def error(self, msg, *args):
        sys.stdout.write("beandispenser (pid {}) error: {} \n".format(os.getpid(), msg.format(*args)))
        sys.stderr.flush()
