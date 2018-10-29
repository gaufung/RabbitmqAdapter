# -*- encoding:utf-8 -*-
"""
tornado.web.RequestHandler's extension.
"""
import json
import logging
from traceback import format_exception

from tornado.log import app_log
from tornado.web import RequestHandler, HTTPError

from common.errors import ASBaseError, _SYS


class BaseHandler(RequestHandler):
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
        if "exc_info" in kwargs and self.settings.get("server_traceback"):
            lines = [line for line in format_exception(*kwargs["exc_info"])]
            self.logger.error("status code: 500; error message: %s", " ".join(lines))
            self.set_status(500)
            self.write({"errId": _SYS, "errMsg": "".join(lines)})
        else:
            if "exc_info" in kwargs and isinstance(kwargs["exc_info"][1], ASBaseError):
                exception = kwargs["exc_info"][1]
                if self.settings["debug"]:
                    app_log.error(exception.message)
                self.logger.error("status code: %s, errId: %s, error message: %s",
                                  str(exception.status_code), str(exception.code), exception.message)
                self.set_status(exception.status_code)
                self.write({"errId": exception.code, "errMsg": exception.message})
            elif "exc_info" in kwargs and isinstance(kwargs["exc_info"][1], HTTPError):
                exception = kwargs["exc_info"][1]
                self.logger.error("status code: %s, errId: %s, error message: %s",
                                  str(exception.status_code), str(_SYS), exception.reason)
                self.set_status(exception.status_code)
                self.write({"errId": _SYS, "errMsg": exception.reason})
            else:
                self.logger.error("status code: %s, errId: %s, error message: %s",
                                  str(status_code), str(_SYS), self._reason)
                self.set_status(status_code)
                self.write({"errId": _SYS, "errMsg": self._reason})

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

