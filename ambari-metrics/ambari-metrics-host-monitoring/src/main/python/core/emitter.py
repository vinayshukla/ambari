#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import logging
import threading
import time
import urllib2

logger = logging.getLogger()
COLLECTOR_URL = "http://{0}/ws/v1/timeline/metrics"
RETRY_SLEEP_INTERVAL = 5
MAX_RETRY_COUNT = 3

class Emitter(threading.Thread):
  """
  Wake up every send interval seconds and empty the application metric map.
  """

  def __init__(self, config, application_metric_map):
    threading.Thread.__init__(self)
    logger.debug('Initializing Emitter thread.')
    self.lock = threading.Lock()
    self.collector_address = config.get_server_address()
    self.send_interval = config.get_send_interval()
    self.application_metric_map = application_metric_map

  def run(self):
    logger.info('Running Emitter thread: %s' % threading.currentThread().getName())
    while True:
      try:
        retry_count = 0
        while retry_count < MAX_RETRY_COUNT:
          self.application_metric_map.acquire_lock()
          json_data = self.application_metric_map.flatten()
          if json_data is None:
            logger.info("Nothing to emit, resume waiting.")
            break
          pass
          response = self.push_metrics(json_data)

          if response and response.status == '201':
            retry_count = MAX_RETRY_COUNT
            self.application_metric_map.clear()
            self.application_metric_map.release_lock()
          else:
            logger.warn("Error sending metrics to server. Retrying after {0} "
                        "...".format(RETRY_SLEEP_INTERVAL))
            self.application_metric_map.release_lock()
            retry_count += 1
            time.sleep(RETRY_SLEEP_INTERVAL)
          pass
        pass

        time.sleep(self.send_interval)
      except Exception, e:
        logger.warn('Unable to emit events. %s' % str(e))
        time.sleep(RETRY_SLEEP_INTERVAL)
        logger.info('Retrying emit after %s seconds.' % RETRY_SLEEP_INTERVAL)
    pass

  def push_metrics(self, data):
    headers = {"Content-Type" : "application/json", "Accept" : "*/*"}
    server = COLLECTOR_URL.format(self.collector_address.strip())
    logger.info("server: %s" % server)
    req = urllib2.Request(server, data, headers)
    response = urllib2.urlopen(req, timeout=int(self.send_interval - 10))
    if response:
      logger.debug("POST response from server: status = {0}, code = {1}".format(
        response.status, response.reason))
      logger.debug(response.read())
    pass
    return response

