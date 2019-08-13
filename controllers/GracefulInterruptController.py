'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
import logging
import signal

class GracefulInterruptController(object):
    """
    Capture signals
    https://stackoverflow.com/questions/1112343/how-do-i-capture-sigint-in-python
    """
    def __init__(self, sig=signal.SIGINT):
        self.logger = logging.getLogger(__name__)
        self.sig = sig
        self.interrupted = False
        self.released = False
        self.original_handler = None

    def __enter__(self):
        self.interrupted = False
        self.released = False
        self.original_handler = signal.getsignal(self.sig)

        def handler(signum, frame):
            self.release()
            self.interrupted = True

        signal.signal(self.sig, handler)
        self.logger.debug('Graceful interrupt controller is listening')
        return self

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):
        self.logger.debug('Graceful interrupt controller is releasing')
        if self.released:
            return False

        signal.signal(self.sig, self.original_handler)
        self.released = True
        return True
