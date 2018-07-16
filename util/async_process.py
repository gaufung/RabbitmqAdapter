# -*- coding:utf-8 -*-
import os
import logging
import signal
from tornado.process import Subprocess
from tornado.gen import Task
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
"""
Asynchronous process call
"""


class AsyncProcess(object):
    @classmethod
    def _get_log(cls, *name):
        return logging.getLogger('.'.join((cls.__module__, cls.__name__) + name))

    def __init__(self, args, cwd=None, timeout=None, is_logging=True,
                 io_loop=None):
        """
        A asynchronous process instance
        :param args: args should be a sequence of program arguments or else a single string
        :type args: list[unix-like system]. e.g. ['/bin/vikings', '-input', 'eggs.txt', '-output', 'spam spam.txt', '-cmd', "echo '$MONEY'"]
        :param cwd: current work directory
        :param timeout: timeout for process (second)
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

    def _timeout_callback(self):
        """
        if timeout, it will be invoked
        :return: None
        """
        log = self._get_log("_timeout_callback")
        if self._process is not None:
            log.info("subprocess: %s will be killed as timeout" % (self._process_name,))
            self._timeout_cancel = None
            os.kill(self._process.pid, signal.SIGKILL)
            log.info("subprocess: %s is killed by system signal" % (self._process_name,))
        else:
            log.error("%s. no such process" % (self._process_name,))

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
        log = self._get_log("_read_log_from_stream")
        log_handler = log.error if is_error else log.info
        try:
            while True:
                line = yield stream.read_until("\n")
                end = -2 if line.endswith("\r\n") else -1
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
        log = self._get_log("invoke")
        if self._is_logging:
            self._process = Subprocess(self._args, cwd=self._cwd, stdout=Subprocess.STREAM,
                                       stderr=Subprocess.STREAM, **kwargs)
        else:
            self._process = Subprocess(self._args, cwd=self._cwd, **kwargs)

        self._process.set_exit_callback(self._exit_callback)
        if self._timeout is not None:
            log.info("invoke timeout")
            self._timeout_cancel = self._io_loop.call_later(self._timeout, self._timeout_callback)

        if self._is_logging:
            log.info("add process stdout and stderr into logging")
            yield [
                Task(self._read_log_from_stream, self._process.stdout),
                Task(self._read_log_from_stream, self._process.stderr, True)
            ]
        if self._timeout_cancel is not None:
            log.info("remove timeout")
            self._io_loop.remove_timeout(self._timeout_cancel)
            self._timeout_cancel = None
        else:
            if self._timeout is not None:
                raise Exception("%s subprocess timeout" % (self._process_name,))
        if self._exit_code is None:
            log.error("%s exit code is None" % (self._process_name,))
        if self._exit_code is not None and self._exit_code != 0:
            if self._exit_code == 2:
                log.error("%s exit code is %d" % (self._process_name, self._exit_code))
            else:
                raise Exception("%s exit code is %d" % (self._process_name, self._exit_code))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _args = ["ls"]
    _io_loop = IOLoop.current()
    async_process = AsyncProcess(_args, cwd=None, timeout=None, is_logging=True, io_loop=_io_loop)
    async_process.invoke()
    IOLoop().current().start()







