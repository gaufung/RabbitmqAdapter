# -*- coding:utf-8 -*-
import logging
import os
import signal

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from tornado import gen
from tornado.gen import Task
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.process import Subprocess


class AsyncProcess(object):
    """
    Asynchronous Process using `tornado.process.Subprocess`. More details see `tornado.process.Subprocess`.
    Usage:
    If shell command is like that: `bash run.sh 10 file-names`, you can use it like this.
    ```
    ap = AsyncProcess(['bash', 'run.sh', '10', 'file-names'])
    ap.invokes()
    ```
    More usage cases, please lookup `test/util_test/test_async_process.py` file.
    """

    def __init__(self, args, cwd=None, timeout=None, is_logging=True,
                 io_loop=None):
        """
        A asynchronous process instance
        :param args: args should be a sequence of program arguments or else a single string
        :type args: list[unix-like system]. e.g. ['/bin/vikings', '-input', 'eggs.txt', '-output', 'spam spam.txt', '-cmd', "echo '$MONEY'"]
        :param cwd: current work directory
        :param timeout: timeout for process (metric: second)
        :param is_logging: whether logging when executing process
        :param io_loop: io loop. if it is None, using IOLoop.current()
        """
        if io_loop is None:
            io_loop = IOLoop.current()
        self._args = args
        self._cwd = cwd
        self._timeout = timeout
        self._is_logging = is_logging
        self._io_loop = io_loop
        self._exit_code = None
        self._process_name = args[0]
        self._process = None
        self._timeout_cancel = None
        self._logger = logging.getLogger(__name__)

    @property
    def logger(self):
        return self._logger

    def _timeout_callback(self):
        """
        if timeout, it will be invoked
        :return: None
        """
        if self._process is not None:
            self.logger.info("subprocess: %s will be killed as timeout" % (self._process_name,))
            self._timeout_cancel = None
            os.kill(self._process.pid, signal.SIGKILL)
            self.logger.info("subprocess: %s is killed by system signal" % (self._process_name,))
        else:
            self.logger.error("%s. no such process" % (self._process_name,))

    def _exit_callback(self, exit_code):
        """
        if process exit, it will be invoked
        :param exit_code: process exit code
        :return: None
        """
        self._exit_code = exit_code

    @gen.coroutine
    def _read_log_from_stream(self, stream, is_error=False):
        """
        read from process's stdout and stderr into logging
        :param stream: stdout or stderr stream
        :param is_error: either stdout or stderr. default is stdout
        :return: None
        """
        log_handler = self.logger.error if is_error else self.logger.info
        try:
            while True:
                line = yield stream.read_until("\n")
                if isinstance(line, str):
                    line = line.decode("utf-8")
                end = -2 if line.endswith(u"\r\n") else -1
                log_handler(line[:end])
        except StreamClosedError:
            pass

    @gen.coroutine
    def invoke(self, **kwargs):
        """
        asynchronous invoke
        :param kwargs: key value arguments for process.
        :return: None
        """
        if self._is_logging:
            self._process = Subprocess(self._args, cwd=self._cwd, stdout=Subprocess.STREAM,
                                       stderr=Subprocess.STREAM, **kwargs)
        else:
            self._process = Subprocess(self._args, cwd=self._cwd, **kwargs)

        self._process.set_exit_callback(self._exit_callback)
        if self._timeout is not None:
            self.logger.info("invoke timeout")
            self._timeout_cancel = self._io_loop.call_later(self._timeout, self._timeout_callback)

        if self._is_logging:
            self.logger.info("add process stdout and stderr into logging")
            yield [
                Task(self._read_log_from_stream, self._process.stdout),
                Task(self._read_log_from_stream, self._process.stderr, True)
            ]
        if self._timeout_cancel is not None:
            self.logger.info("remove timeout")
            self._io_loop.remove_timeout(self._timeout_cancel)
            self._timeout_cancel = None
        else:
            if self._timeout is not None:
                raise Exception("%s subprocess timeout" % (self._process_name,))
        ret = yield self._process.wait_for_exit(False)
        self.logger.info("process exit code: %d", ret)
        if ret != 0:
            raise Exception("process exit code %d", ret)


class AsyncProcessPool(object):
    def __init__(self, pool_size):
        self._pool = ProcessPoolExecutor(max_workers=pool_size)

    @gen.coroutine
    def submit(self, fn, *args, **kwargs):
        yield self._pool.submit(fn, *args, **kwargs)
        raise gen.Return(None)

    def shutdown(self, wait=True):
        self._pool.shutdown(wait)


class AsyncThreadPool(object):
    def __init__(self, pool_size):
        self._pool = ThreadPoolExecutor(pool_size)

    @gen.coroutine
    def submit(self, fn, *args, **kwargs):
        value = yield self._pool.submit(fn, *args, **kwargs)
        raise gen.Return(value)

    def shutdown(self, wait=True):
        self._pool.shutdown(wait)

