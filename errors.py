# -*- coding: utf-8 -*-
"""
    errors:
    error classes for ASR trainer
"""

_ARG_FORMAT = 1
_ALREADY_EXIST = 2
_NOT_EXIST = 3
_NOT_READY = 4
_NOT_LOGIN = 5
_OWNER_MISMATCH = 6
_EMPTY_SET = 7
_NOT_SUPPORT = 8
_SERVER_BUSY = 9
_OPERATION_FORBIDDEN = 10
_PERMISSION_DENIED = 11
_ARG_EXPIRED = 12
_RESOURCE_BUSY = 13
_SYS = 100


class ASBreak(Exception):
    pass


class ASBaseError(Exception):
    """
    AS base Error
    """
    def __init__(self, code, msg, status_code=200):
        self._code = code
        self._msg = msg
        self._status_code = status_code

    @property
    def status_code(self):
        """
        error of http status code
        :return: http status code
        """
        return self._status_code

    @property
    def code(self):
        """
        error defined code
        :return: code
        """
        return self._code

    @property
    def message(self):
        """
        error message
        :return: message
        """
        return self._msg


class ASArgFormatError(ASBaseError):
    """
    AS argument format error
    """
    def __init__(self, msg):
        super(ASArgFormatError, self).__init__(_ARG_FORMAT, msg, 400)


class ASAlreadyExistError(ASBaseError):
    """
    AS already exist error
    """
    def __init__(self, msg):
        super(ASAlreadyExistError, self).__init__(_ALREADY_EXIST, msg, 400)


class ASNotExistError(ASBaseError):
    """
    AS not exist error
    """
    def __init__(self, msg):
        super(ASNotExistError, self).__init__(_NOT_EXIST, msg, 400)


class ASNotReadyError(ASBaseError):
    """
    AS not ready error
    """
    def __init__(self, msg):
        super(ASNotReadyError, self).__init__(_NOT_READY, msg, 500)


class ASNotLoginError(ASBaseError):
    """
    AS not login error
    """
    def __init__(self, msg):
        super(ASNotLoginError, self).__init__(_NOT_LOGIN, msg, 400)


class ASOwnerMismatchError(ASBaseError):
    """
    As owner mismatch error
    """
    def __init__(self, msg):
        super(ASOwnerMismatchError, self).__init__(_OWNER_MISMATCH, msg, 400)


class ASEmptySetError(ASBaseError):
    """
    AS empty set error
    """
    def __init__(self, msg):
        super(ASEmptySetError, self).__init__(_EMPTY_SET, msg, 400)


class ASNotSupportError(ASBaseError):
    """
    AS not support error
    """
    def __init__(self, msg):
        super(ASNotSupportError, self).__init__(_NOT_SUPPORT, msg, 400)


class ASServerBusyError(ASBaseError):
    """
    As server busy error
    """
    def __init__(self, msg):
        super(ASServerBusyError, self).__init__(_SERVER_BUSY, msg, 500)


class ASOperationForbiddenError(ASBaseError):
    """
    AS operation forbidden error
    """
    def __init__(self, msg):
        super(ASOperationForbiddenError, self).__init__(_OPERATION_FORBIDDEN, msg, 400)


class ASPermissionDeniedError(ASBaseError):
    """
    AS permission denied error
    """
    def __init__(self, msg):
        super(ASPermissionDeniedError, self).__init__(_PERMISSION_DENIED, msg, 400)


class ASArgExpiredError(ASBaseError):
    """
    AS argument expired error
    """
    def __init__(self, msg):
        super(ASArgExpiredError, self).__init__(_ARG_EXPIRED, msg, 400)


class ASResourceBusyError(ASBaseError):
    """
    AS resource busy error
    """
    def __init__(self, msg):
        super(ASResourceBusyError, self).__init__(_RESOURCE_BUSY, msg, 500)


class ASSysError(ASBaseError):
    """
    AS system error
    """
    def __init__(self, msg):
        super(ASSysError, self).__init__(_SYS, msg, 500)

    @classmethod
    def error_code(cls):
        return _SYS
