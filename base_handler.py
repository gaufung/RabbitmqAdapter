# -*- encoding:utf-8 -*-
"""
base handler for asr train request handler
"""
import json
import logging
from traceback import format_exception

from tornado.log import app_log
from tornado.web import RequestHandler, HTTPError

from common.errors import ASBaseError, ASSysError


class BaseHandler(RequestHandler):
    """
    Base handler
    """
    def __init__(self, application, request, **kwargs):
        super(BaseHandler, self).__init__(application, request, **kwargs)
        self._logger = logging.getLogger(__name__)

    @property
    def logger(self):
        """
        The logger
        :return:
        """
        return self._logger

    def log_exception(self, typ, value, tb):
        if not self.settings["debug"] and isinstance(value, ASBaseError):
            return
        super(BaseHandler, self).log_exception(typ, value, tb)

    def write_error(self, status_code, **kwargs):
        """
        override `RequestHandler` write_error method. need to be considered again.
        :param status_code:
        :param kwargs:
        :return:
        """
        if "exc_info" in kwargs and isinstance(kwargs["exc_info"][1], ASBaseError):
            exception = kwargs["exc_info"][1]
            if self.settings["debug"]:
                app_log.error(exception.message)
            self.write_failure(exception.code, exception.message, exception.status_code)
        else:
            if self.settings["server_traceback"] and "exc_info" in kwargs:
                lines = [line for line in format_exception(*kwargs["exc_info"])]
                self.write_failure(ASSysError.error_code(), "".join(lines))
            elif "exc_info" in kwargs and not isinstance(kwargs["exc_info"][1], HTTPError):
                self.write_failure(ASSysError.error_code(), str(kwargs["exc_info"][1]))
            else:
                self.write_failure(ASSysError.error_code(), self._reason, status_code)

    def write_json(self, data, file_name=None):
        """
        write data to client as json. if `file_name` is set, write as json file.
        :param data: data to be written
        :param file_name: file name
        :return: None
        """
        if data is None:
            raise Exception("data is None, but it wants to send it as json.")
        data = json.dumps(data, ensure_ascii=False)
        self.write(data)
        self.set_header("Content-Type", "application/json;charset=utf-8")
        if file_name is not None:
            self.set_header("Content-Disposition", "attachment; filename=%s" % file_name)
        self.finish()

    def write_success(self, data=None):
        """
        write success information to client
        :param data: if it is None, write empty dictionary
        :return: None
        """
        self.set_header('Access-Control-Allow-Origin', '*')
        if data is None:
            data = {}
        self.write(data)
        self.finish()

    def write_failure(self, err_code, err_msg, status_code=None):
        """
        write failure information to client
        :param err_code: error code
        :param err_msg: error message
        :param status_code: http status code
        :return: None
        """
        self.set_header('Access-Control-Allow-Origin', '*')
        if status_code is not None:
            self.set_status(status_code)
        self.write({"errId": err_code, "errMsg": err_msg})
        self.finish()
