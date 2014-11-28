import sys

class Logger(object):

    def info(self, msg, *args):
        sys.stdout.write("beandispenser info: " + msg.format(*args) + "\n")
        sys.stderr.flush()

    def error(self, msg, *args):
        sys.stderr.write("beandispenser error: " + msg.format(*args) + "\n")
        sys.stderr.flush()
