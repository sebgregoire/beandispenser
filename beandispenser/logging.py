import sys

class Logger(object):

    def info(self, msg, *args):
        sys.stdout.write(msg.format(*args) + "\n")
        sys.stderr.flush()

    def error(self, msg, *args):
        sys.stderr.write(msg.format(*args) + "\n")
        sys.stderr.flush()
