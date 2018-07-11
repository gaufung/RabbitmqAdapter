# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import unittest
import util.errors
from util.errors import *


class TestASError(unittest.TestCase):
    def test_ASError(self):
        test_cases = [
            {
                "error":ASArgFormatError("as_arg_format_error"),
                "message" : "as_arg_format_error",
                "code" : util.errors._ARG_FORMAT,
                "status_code": 400,
            },
            {
                "error": ASAlreadyExistError("as_already_exist_error"),
                "message": "as_already_exist_error",
                "code": util.errors._ALREADY_EXIST,
                "status_code": 400,
            },
            {
                "error": ASNotExistError("as_not_exist_error"),
                "message": "as_not_exist_error",
                "code": util.errors._NOT_EXIST,
                "status_code": 400,
            },
            {
                "error": ASNotReadyError("as_not_ready_error"),
                "message": "as_not_ready_error",
                "code": util.errors._NOT_READY,
                "status_code": 500,
            },
            {
                "error": ASNotLoginError("as_not_login_error"),
                "message": "as_not_login_error",
                "code": util.errors._NOT_LOGIN,
                "status_code": 400,
            },
            {
                "error": ASOwnerMismatchError("as_owner_mismatch_error"),
                "message": "as_owner_mismatch_error",
                "code": util.errors._OWNER_MISMATCH,
                "status_code": 400,
            },
            {
                "error": ASEmptySetError("as_empty_set_error"),
                "message": "as_empty_set_error",
                "code": util.errors._EMPTY_SET,
                "status_code": 400,
            },
            {
                "error": ASNotSupportError("as_not_support_error"),
                "message": "as_not_support_error",
                "code": util.errors._NOT_SUPPORT,
                "status_code": 400,
            },
            {
                "error": ASServerBusyError("as_server_busy_error"),
                "message": "as_server_busy_error",
                "code": util.errors._SERVER_BUSY,
                "status_code": 500,
            },
            {
                "error": ASOperationForbiddenError("as_operation_forbidden_error"),
                "message": "as_operation_forbidden_error",
                "code": util.errors._OPERATION_FORBIDDEN,
                "status_code": 400,
            },
            {
                "error": ASPermissionDeniedError("as_permission_denied_error"),
                "message": "as_permission_denied_error",
                "code": util.errors._PERMISSION_DENIED,
                "status_code": 400,
            },
            {
                "error": ASArgExpiredError("as_arg_expired_error"),
                "message": "as_arg_expired_error",
                "code": util.errors._ARG_EXPIRED,
                "status_code": 400,
            },
            {
                "error": ASResourceBusyError("as_resource_busy_error"),
                "message": "as_resource_busy_error",
                "code": util.errors._RESOURCE_BUSY,
                "status_code": 500,
            },
            {
                "error": ASSysError("as_sys_error"),
                "message": "as_sys_error",
                "code": util.errors._SYS,
                "status_code": 500,
            },
        ]
        for tt in test_cases:
            error = tt["error"]
            self.assertEqual(error.code, tt["code"])
            self.assertEqual(error.message, tt["message"])
            self.assertEqual(error.status_code, tt["status_code"])


if __name__ == "__main__":
    unittest.main()