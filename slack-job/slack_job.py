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

import logging.config
import os
import re
import argparse
import base64
from slack import WebClient
from slack.errors import SlackApiError


def send_message_to_slack(client, addressees, message):
    """
    Sending message to slack to specific recipients

    :param client: web client
    :param addressees: recipients receiving the message
    :type addressees list of str
    :param message: message that will send o recipients
    :type message str
    """
    def replace_placeholder(address):
        placeholder = re.search(r'^#([A-Za-z0-9\\-_]{1,50})#$', address)
        if placeholder:
            return os.environ[placeholder.group(1)]
        return address

    is_success = True
    for addressee in addressees:
        try:
            addr = replace_placeholder(addressee)
            channel_id = get_channel_id(client, addr)
            if not channel_id:
                is_success = False
                continue

            client.chat_postMessage(channel=channel_id, text=message)
            LOGGER.info('Message successfully sent to %s', channel_id)
        except SlackApiError:
            is_success = False
            LOGGER.exception('The message was not sent')
        except KeyError:
            is_success = False
            LOGGER.exception('Wrong address')
    return is_success


def get_channel_id(client, identifier):
    """
    Getting channel id if identifier is email
    :param client: web client
    :param identifier: email or channel id
    :type identifier: str
    :return: channel id
    :rtype: str
    """
    try:
        if "@" in identifier:
            return client.users_lookupByEmail(email=identifier)["user"]["id"]
        return identifier
    except SlackApiError:
        LOGGER.exception('Receiving person id failed')


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-message", "-m", help="message to send", required=True)
    parser.add_argument("-addresses", "-a", nargs='+', help="addresses who to send message", required=True)
    args = parser.parse_args()

    _client = WebClient(token=os.environ['SLACK_API_TOKEN'])
    is_success = send_message_to_slack(_client, args.addresses, args.message)
    if not is_success:
        raise RuntimeError('There was an exception on sending')


if __name__ == '__main__':
    logging.config.fileConfig(os.environ['LOGGER_CONFIG_FILE_PATH'])
    LOGGER = logging.getLogger()

    run()
