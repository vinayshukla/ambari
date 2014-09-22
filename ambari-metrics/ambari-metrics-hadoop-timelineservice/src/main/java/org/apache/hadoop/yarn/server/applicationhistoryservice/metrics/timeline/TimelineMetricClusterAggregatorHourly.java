/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TimelineMetricClusterAggregatorHourly extends AbstractTimelineAggregator {
  private static final Log LOG = LogFactory.getLog(TimelineMetricClusterAggregator.class);
  public static final long SLEEP_INTERVAL = 3600000;

  public TimelineMetricClusterAggregatorHourly(PhoenixHBaseAccessor hBaseAccessor,
                                               String checkpointLocation) {
    super(hBaseAccessor, checkpointLocation);
  }

  @Override
  protected boolean doWork(long startTime, long endTime) {
    return false;
  }

  @Override
  protected Long getSleepInterval() {
    return SLEEP_INTERVAL;
  }

  @Override
  protected Long getCheckpointCutOffInterval() {
    return 7200000l;
  }


}
