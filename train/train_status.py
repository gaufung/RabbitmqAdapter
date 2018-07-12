# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import time
import logging


class StageTimeout(object):
    """
    timeout configuration
    contains:
        - train timeout
        - public timeout
        - checkout timeout
    """
    TRAIN_TIMEOUT = 60 * 60
    PUBLIC_TIMEOUT = 60 * 10
    CHECK_TIMEOUT = 60


class StageRequest(object):
    """
    Stage request
    contains:
        - error msg
        - callback
        - lm name (language model name)
        - lm id (language model id)
    """
    ERROR_MSG = "errorMsg"
    CALLBACK = "callback"
    LM_NAME = "lmName"
    LM_ID = "lmId"


class StageTime(object):
    """
    Stage time
    contains:
        - start time
        - end time
        - train start time
        - publish start time
        - check start time
        - predict train time
    """
    START_TIME = "startTime"
    END_TIME = "endTime"
    TRAIN_START_TIME = "strainStartTime"
    PUBLISH_START_TIME = 'publishStartTime'
    CHECK_START_TIME = "checkStartTime"
    PREDICT_TRAIN_TIME = "predictTrainTime"


class StageName(object):
    """
    Stage Name
    contains:
        - current step
        - step pending
        - step training
        - step request publishing
        - step publishing
        - step checking
    """
    CURRENT_STEP = "currentStep"
    STEP_PENDING = "pending"
    STEP_TRAINING = "training"
    STEP_REQUEST_PUBLISHING = "request_publishing"
    STEP_PUBLISHING = "publishing"
    STEP_CHECKING = "checking"


class StageResponse(object):
    """
    Stage response
    contains
        - Step success
        - Step failed
    """
    STEP_SUCCESS = "success"
    STEP_FAILED = "failed"


class StageFlag(object):
    """
    Stage flag
    contains:
        - train flag
        - flag builtin
        - flag custom
    """
    TRAIN_FLAG = "trainFlag"
    FLAG_BUILTIN = "builtin"
    FLAG_CUSTOM = "custom"


class Db(object):
    """
    Abstract Database class. including get and set set method, need to be override.
    """
    def get(self, key):
        raise NotImplementedError

    def set(self, key, value):
        raise NotImplementedError


class ASRTrainStatus(object):
    @classmethod
    def _get_log(cls, *name):
        """
        return logger
        :param name: name to be logged
        :return: a logger
        """
        return logging.getLogger('.'.join((cls.__module__, cls.__name__) + name))

    def __init__(self, db):
        """
        instance of ASRTrainStatus
        :param db: must be a subclass of Db instance
        """
        if not isinstance(db, Db):
            raise Exception("db is not instance of train.train_status.Db'subclass ")
        self._db = db

    def get(self, request_id):
        """
        train status of given `request_id`
        :param request_id:
        :return: [dict] status
        """
        log = self._get_log("get")

        def _refine(data, key, expect_value_type, is_must=False):
            """
            refine data, type conversion of value by given key.
            Attention: it has side effect
            :param data: dictionary
            :param key: given key
            :param expect_value_type: expect value type
            :param is_must: is this key needed
            :return: None
            """
            if is_must and key not in data:
                log.error("key %s not in data " % (key,))
                raise Exception("key: %s not in data" % key)
            if key in data:
                data[key] = expect_value_type(data[key])
        log.info("request_id: %s" % (request_id,))
        result = self._db.get(request_id)
        if not result:
            log.info("request_id %s status is empty" % (request_id,))
            return
        if StageFlag.TRAIN_FLAG not in result:
            result[StageFlag.TRAIN_FLAG] = StageFlag.FLAG_CUSTOM
        _refine(result, StageName.CURRENT_STEP, str, True)
        _refine(result, StageRequest.ERROR_MSG, str, True)
        _refine(result, StageTime.START_TIME, str, True)
        _refine(result, StageRequest.CALLBACK, str, True)
        _refine(result, StageFlag.TRAIN_FLAG, str, True)
        _refine(result, StageRequest.LM_NAME, str, False)
        _refine(result, StageRequest.LM_ID, str, False)
        _refine(result, StageTime.TRAIN_START_TIME, float, False)
        _refine(result, StageTime.PUBLISH_START_TIME, float, False)
        _refine(result, StageTime.PREDICT_TRAIN_TIME, float, False)
        _refine(result, StageTime.END_TIME, float, False)
        return result

    def start_train(self, request_id, callback, lm_name, train_flag):
        """
        update train start
        :param request_id: request id
        :param callback: callback url
        :param lm_name: language name
        :param train_flag: train flag
        :return: None
        """
        log = self._get_log("start_train")
        log.info("request_id: %s; callback: %s; lm_name: %s; train_flag; %s" %
                 (request_id, callback, lm_name, train_flag,))
        value = {
            StageName.CURRENT_STEP: StageName.STEP_PENDING,
            StageRequest.ERROR_MSG: "",
            StageTime.START_TIME: time.time(),
            StageRequest.CALLBACK: callback,
            StageRequest.LM_NAME: lm_name,
            StageFlag.TRAIN_FLAG: train_flag,
        }
        self._db.set(request_id, value)

    def update_predict(self, request_id, predict_train_time):
        """
        update train status
        :param request_id: request id
        :param predict_train_time: predict train time
        :return: None
        """
        log = self._get_log("update_predict")
        log.info("request_id: %s; predict_train_time: %s" % (request_id, predict_train_time))
        value = {
            StageName.CURRENT_STEP: StageName.STEP_PENDING,
            StageTime.TRAIN_START_TIME: time.time(),
            StageTime.PREDICT_TRAIN_TIME: predict_train_time
        }
        self._db.set(request_id, value)

    def request_publish(self, request_id, lm_id):
        """
        request publish
        :param request_id: request id
        :param lm_id: lmd id
        :return: None
        """
        log = self._get_log("request_publish")
        log.info("request_id: %s; lm_id: %s" % (request_id, lm_id,))
        value = {
            StageName.CURRENT_STEP: StageName.STEP_REQUEST_PUBLISHING,
            StageTime.PUBLISH_START_TIME: time.time(),
            StageRequest.LM_ID: lm_id,
        }
        self._db.set(request_id, value)

    def public_train(self, request_id):
        """
        public train
        :param request_id: request id
        :return: None
        """
        log = self._get_log("public_train")
        log.info("request_id %s" % request_id)
        value = {
            StageName.CURRENT_STEP: StageName.STEP_PUBLISHING,
        }
        self._db.set(request_id, value)

    def error_train(self, request_id, error_msg):
        """
        send error to train
        :param request_id: request error
        :param error_msg: error message
        :return: None
        """
        log = self._get_log("error_train")
        log.info("request_id: %s; error_msg: %s" % (request_id, error_msg,))
        value = {
            StageName.CURRENT_STEP: StageResponse.STEP_FAILED,
            StageRequest.ERROR_MSG: error_msg,
            StageTime.END_TIME: time.time(),
        }
        self._db.set(request_id, value)

    def finish_train(self, request_id):
        """
        finish train
        :param request_id: request id
        :return:None
        """
        log = self._get_log("finish_train")
        log.info("request_id: %s" % (request_id,))
        value = {
            StageName.CURRENT_STEP: StageResponse.STEP_SUCCESS,
            StageTime.END_TIME: time.time(),
        }
        self._db.set(request_id, value)

    def check_train(self, request_id):
        """
        check train status
        :param request_id:  request id
        :return: None
        """
        log = self._get_log("check_train")
        log.info("request_id %s" % request_id)
        value = {
            StageName.CURRENT_STEP: StageName.STEP_CHECKING,
            StageTime.CHECK_START_TIME: time.time()
        }
        self._db.set(request_id, value)
