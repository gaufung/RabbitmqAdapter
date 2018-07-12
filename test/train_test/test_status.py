# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import unittest
import time
from train.train_status import Db, ASRTrainStatus, StageResponse, StageName, StageRequest, StageTime, StageFlag, StageTimeout


class MemoryDb(Db):
    """
    Memory
    """
    def __init__(self):
        self.warehouse = {}

    def get(self, key):
        return self.warehouse[key]

    def set(self, key, value):
        if key not in self.warehouse:
            self.warehouse[key] = value
        else:
            for k, v in value.items():
                self.warehouse[key][k] = v


class TestASRTrainStatus(unittest.TestCase):
    def setUp(self):
        self.asrTrainStatus = ASRTrainStatus(MemoryDb())
        self.request_id = "23d1"
        self.callback = "http://www.dui.ai/callback"
        self.lm_name = "language_model"
        train_flag = StageFlag.FLAG_BUILTIN
        self.asrTrainStatus.start_train(self.request_id, self.callback, self.lm_name, train_flag)
        status = self.asrTrainStatus.get(self.request_id)
        self.assertEqual(status[StageName.CURRENT_STEP], StageName.STEP_PENDING)
        self.assertEqual(status[StageRequest.ERROR_MSG], "")
        self.assertEqual(status[StageRequest.CALLBACK], self.callback)
        self.assertEqual(status[StageRequest.LM_NAME], self.lm_name)
        self.assertEqual(status[StageFlag.TRAIN_FLAG], StageFlag.FLAG_BUILTIN)

    def test_update_predict(self):
        predict_train_time = time.time()
        self.asrTrainStatus.update_predict(self.request_id, predict_train_time)
        status = self.asrTrainStatus.get(self.request_id)
        self.assertAlmostEqual(status[StageTime.PREDICT_TRAIN_TIME], predict_train_time)
        self.assertEqual(status[StageName.CURRENT_STEP], StageName.STEP_PENDING)

    def test_request_public(self):
        lm_id = "adfa234d"
        self.asrTrainStatus.request_publish(self.request_id, lm_id)
        status = self.asrTrainStatus.get(self.request_id)
        self.assertEqual(status[StageName.CURRENT_STEP], StageName.STEP_REQUEST_PUBLISHING)
        self.assertEqual(status[StageRequest.LM_ID], lm_id)

    def test_public_train(self):
        self.asrTrainStatus.public_train(self.request_id)
        status = self.asrTrainStatus.get(self.request_id)
        self.assertEqual(status[StageName.CURRENT_STEP], StageName.STEP_PUBLISHING)

    def test_error_msg(self):
        self.asrTrainStatus.error_train(self.request_id, "Nothing")
        status = self.asrTrainStatus.get(self.request_id)
        self.assertEqual(status[StageName.CURRENT_STEP], StageResponse.STEP_FAILED)
        self.assertEqual(status[StageRequest.ERROR_MSG], "Nothing")

    def test_finish_train(self):
        self.asrTrainStatus.finish_train(self.request_id)
        status = self.asrTrainStatus.get(self.request_id)
        self.assertEqual(status[StageName.CURRENT_STEP], StageResponse.STEP_SUCCESS)

    def test_check_train(self):
        self.asrTrainStatus.check_train(self.request_id)
        status = self.asrTrainStatus.get(self.request_id)
        self.assertEqual(status[StageName.CURRENT_STEP], StageName.STEP_CHECKING)


if __name__ == "__main__":
    unittest.main()