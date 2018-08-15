# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import unittest
import errors



class TestASError(unittest.TestCase):
    def test_ASError(self):
        test_cases = [
            {
                "error": errors.ASArgFormatError("as_arg_format_error"),
                "message" : "as_arg_format_error",
                "code" : errors._ARG_FORMAT,
                "status_code": 400,
            },
            {
                "error": errors.ASAlreadyExistError("as_already_exist_error"),
                "message": "as_already_exist_error",
                "code": errors._ALREADY_EXIST,
                "status_code": 400,
            },
            {
                "error": errors.ASNotExistError("as_not_exist_error"),
                "message": "as_not_exist_error",
                "code": errors._NOT_EXIST,
                "status_code": 400,
            },
            {
                "error": errors.ASNotReadyError("as_not_ready_error"),
                "message": "as_not_ready_error",
                "code": errors._NOT_READY,
                "status_code": 500,
            },
            {
                "error": errors.ASNotLoginError("as_not_login_error"),
                "message": "as_not_login_error",
                "code": errors._NOT_LOGIN,
                "status_code": 400,
            },
            {
                "error": errors.ASOwnerMismatchError("as_owner_mismatch_error"),
                "message": "as_owner_mismatch_error",
                "code": errors._OWNER_MISMATCH,
                "status_code": 400,
            },
            {
                "error": errors.ASEmptySetError("as_empty_set_error"),
                "message": "as_empty_set_error",
                "code": errors._EMPTY_SET,
                "status_code": 400,
            },
            {
                "error": errors.ASNotSupportError("as_not_support_error"),
                "message": "as_not_support_error",
                "code": errors._NOT_SUPPORT,
                "status_code": 400,
            },
            {
                "error": errors.ASServerBusyError("as_server_busy_error"),
                "message": "as_server_busy_error",
                "code": errors._SERVER_BUSY,
                "status_code": 500,
            },
            {
                "error": errors.ASOperationForbiddenError("as_operation_forbidden_error"),
                "message": "as_operation_forbidden_error",
                "code": errors._OPERATION_FORBIDDEN,
                "status_code": 400,
            },
            {
                "error": errors.ASPermissionDeniedError("as_permission_denied_error"),
                "message": "as_permission_denied_error",
                "code": errors._PERMISSION_DENIED,
                "status_code": 400,
            },
            {
                "error": errors.ASArgExpiredError("as_arg_expired_error"),
                "message": "as_arg_expired_error",
                "code": errors._ARG_EXPIRED,
                "status_code": 400,
            },
            {
                "error": errors.ASResourceBusyError("as_resource_busy_error"),
                "message": "as_resource_busy_error",
                "code": errors._RESOURCE_BUSY,
                "status_code": 500,
            },
            {
                "error": errors.ASSysError("as_sys_error"),
                "message": "as_sys_error",
                "code": errors._SYS,
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