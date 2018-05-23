import threading
import signal


class SignalExitter:
    def __init__(self):
        self.event = threading.Event()
        signal.signal(signal.SIGINT, self._exit_by_signal)
        signal.signal(signal.SIGTERM, self._exit_by_signal)

    def _exit_by_signal(self, signum, frame):
        self.event.set()

    def wait(self, timeout=None):
        return self.event.wait(timeout)
