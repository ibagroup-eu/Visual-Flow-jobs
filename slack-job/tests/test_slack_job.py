#
# Copyright (c) 2021 IBA Group, a.s. All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
from unittest.mock import patch, call, MagicMock

from slack.errors import SlackApiError


class TestSlackJob(unittest.TestCase):
    def test_run(self):
        with patch('slack_job.send_message_to_slack') as send, patch(
                'slack_job.argparse'), patch.dict('os.environ', values={'SLACK_API_TOKEN': 'token'}):
            import slack_job
            slack_job.run()
            send.assert_called_once()

    def test_run_exception(self):
        with patch('slack_job.send_message_to_slack') as send, patch(
                'slack_job.argparse'), patch.dict('os.environ', values={'SLACK_API_TOKEN': 'token'}):
            import slack_job
            send.return_value = False
            self.assertRaises(RuntimeError, slack_job.run)
            send.assert_called_once()

    def test_send_message_to_slack(self):
        with patch('slack_job.get_channel_id') as get_channel:
            import slack_job
            client = MagicMock()
            slack_job.LOGGER = MagicMock()

            get_channel.side_effect = ['addressee1Id', 'addressee2Id']
            self.assertTrue(
                slack_job.send_message_to_slack(client, ['addressee1', 'addressee2@gomel.iba.by'], 'message'))

            get_channel_id_calls = [call(client, 'addressee1'), call(client, 'addressee2@gomel.iba.by')]
            get_channel.assert_has_calls(get_channel_id_calls)

            chat_post_message_calls = [call(channel='addressee1Id', text='message'),
                                       call(channel='addressee2Id', text='message')]
            client.chat_postMessage.assert_has_calls(chat_post_message_calls)

    def test_send_message_to_slack_exception(self):
        with patch('slack_job.get_channel_id') as get_channel:
            import slack_job
            client = MagicMock()
            slack_job.LOGGER = MagicMock()

            get_channel.side_effect = ['addressee1Id', 'addressee2Id']

            client.chat_postMessage.side_effect = SlackApiError('', '')

            self.assertFalse(
                slack_job.send_message_to_slack(client, ['addressee1', 'addressee2@gomel.iba.by'], 'message'))

            get_channel_id_calls = [call(client, 'addressee1'), call(client, 'addressee2@gomel.iba.by')]
            get_channel.assert_has_calls(get_channel_id_calls)

    def test_send_get_channel_id_by_email(self):
        import slack_job
        client = MagicMock()

        client.users_lookupByEmail.return_value = {"user": {"id": "user_id"}}

        self.assertEqual(slack_job.get_channel_id(client, 'identifier@gomel.iba.by'), 'user_id')

    def test_send_get_channel_id_by_channel_name(self):
        import slack_job
        client = MagicMock()

        self.assertEqual(slack_job.get_channel_id(client, 'identifier'), 'identifier')

    def test_send_get_channel_id_by_email_exception(self):
        import slack_job
        slack_job.LOGGER = MagicMock()
        client = MagicMock()

        client.users_lookupByEmail.side_effect = SlackApiError('', '')

        self.assertIsNone(slack_job.get_channel_id(client, 'identifier@gomel.iba.by'), 'user_id')
