# -*- coding:utf-8 -*-

# Copyright (c) 2016, Botify
#
# See the file LICENSE for copying permission.

from simpleflow.utils import json_dumps
from swf.models.decision.base import Decision, decision_action


class LambdaFunctionDecision(Decision):
    _base_type = 'LambdaFunction'

    @decision_action
    def schedule(self, id, name, input=None, start_to_close_timeout=None):
        """Schedule lambda function decision builder

        :param  id: id of the Lambda function
        :type   id: str

        :param  name: name of the Lambda function to schedule
        :type   name: str

        :param  input: input provided to the activity task
        :type   input: Optional[dict]

        :param  start_to_close_timeout: timeout, 1-300 seconds. Default: 300
        :type   start_to_close_timeout: Optional[str]
        """
        if input is not None:
            input = json_dumps(input)

        self.update_attributes({
            'id': id,
            'name': name,
            'input': input,
            'startToCloseTimeout': start_to_close_timeout,
        })
