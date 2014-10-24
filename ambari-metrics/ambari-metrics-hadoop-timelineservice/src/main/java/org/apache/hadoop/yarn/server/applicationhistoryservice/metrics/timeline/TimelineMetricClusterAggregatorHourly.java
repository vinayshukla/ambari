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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.TimelineMetricConfiguration.CLUSTER_AGGREGATOR_HOUR_CHECKPOINT_CUTOFF_MULTIPLIER;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.TimelineMetricConfiguration.CLUSTER_AGGREGATOR_HOUR_SLEEP_INTERVAL;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.TimelineMetricConfiguration.DEFAULT_CHECKPOINT_LOCATION;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.TimelineMetricConfiguration.TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR;

public class TimelineMetricClusterAggregatorHourly extends AbstractTimelineAggregator {
  private static final Log LOG = LogFactory.getLog(TimelineMetricClusterAggregatorHourly.class);
  private final long sleepInterval;
  private static final String CLUSTER_AGGREGATOR_HOURLY_CHECKPOINT_FILE =
    "timeline-metrics-cluster-aggregator-hourly-checkpoint";
  private final String checkpointLocation;
  private final Integer checkpointCutOffMultiplier;

  public TimelineMetricClusterAggregatorHourly(PhoenixHBaseAccessor hBaseAccessor,
                                               Configuration metricsConf) {
    super(hBaseAccessor, metricsConf);

    String checkpointDir = metricsConf.get(
      TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);

    checkpointLocation = FilenameUtils.concat(checkpointDir,
      CLUSTER_AGGREGATOR_HOURLY_CHECKPOINT_FILE);

    sleepInterval = metricsConf.getLong(CLUSTER_AGGREGATOR_HOUR_SLEEP_INTERVAL, 3600000l);
    checkpointCutOffMultiplier =
      metricsConf.getInt(CLUSTER_AGGREGATOR_HOUR_CHECKPOINT_CUTOFF_MULTIPLIER, 2);
  }

  @Override
  protected String getCheckpointLocation() {
    return checkpointLocation;
  }

  @Override
  protected boolean doWork(long startTime, long endTime) {
    return false;
  }

  @Override
  protected Long getSleepInterval() {
    return sleepInterval;
  }

  @Override
  protected Integer getCheckpointCutOffMultiplier() {
    return checkpointCutOffMultiplier;
  }

  @Override
  protected Long getCheckpointCutOffInterval() {
    return 7200000l;
  }


}
