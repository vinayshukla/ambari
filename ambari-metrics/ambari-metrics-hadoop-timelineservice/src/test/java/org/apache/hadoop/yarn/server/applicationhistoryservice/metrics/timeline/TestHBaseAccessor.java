package org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetrics;
import org.junit.*;

import java.sql.*;
import java.util.*;

import static junit.framework.Assert.*;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.PhoenixTransactSQL.*;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHBaseAccessor {
  private static String MY_LOCAL_URL =
    "jdbc:phoenix:c6503.ambari.apache.org:" + 2181 + ":/hbase";
  private Connection conn;
  private PhoenixHBaseAccessor hdb;


  @Before
  public void setUp() throws Exception {
    hdb = createTestableHBaseAccessor();
    conn = getConnection(getUrl());
    Statement stmt = conn.createStatement();

    hdb.initMetricSchema();
  }

  @After
  public void tearDown() throws Exception {
    Connection conn = getConnection(getUrl());
    Statement stmt = conn.createStatement();

    stmt.execute("delete from METRIC_AGGREGATE");
    stmt.execute("delete from METRIC_AGGREGATE_HOURLY");
    stmt.execute("delete from METRIC_RECORD");
    stmt.execute("delete from METRIC_RECORD_HOURLY");
    stmt.execute("delete from METRIC_RECORD_MINUTE");
    conn.commit();

    stmt.close();
    conn.close();
  }
  /**
   * A canary test.
   *
   * @throws Exception
   */
  @Test
  public void testClusterOK() throws Exception {
    Connection conn = getConnection(getUrl());
    Statement stmt = conn.createStatement();
    String sampleDDL = "CREATE TABLE TEST_METRICS (TEST_COLUMN VARCHAR " +
      "CONSTRAINT pk PRIMARY KEY (TEST_COLUMN)) " +
      "DATA_BLOCK_ENCODING='FAST_DIFF', " +
      "IMMUTABLE_ROWS=true, TTL=86400, COMPRESSION='SNAPPY'";

    stmt.executeUpdate(sampleDDL);
    ResultSet rs = stmt.executeQuery(
      "SELECT COUNT(TEST_COLUMN) FROM TEST_METRICS");

    rs.next();
    long l = rs.getLong(1);
    assertThat(l).isGreaterThanOrEqualTo(0);

    stmt.execute("DROP TABLE TEST_METRICS");
  }

  //  @Test
  public void testShouldInsertMetrics() throws Exception {
    // GIVEN

    // WHEN
    long startTime = System.currentTimeMillis();
    TimelineMetrics metricsSent = prepareTimelineMetrics(startTime, "local");
    hdb.insertMetricRecords(metricsSent);

    Condition queryCondition = new Condition(null, "local", null, null,
      startTime, startTime + (15 * 60 * 1000), null, false);
    TimelineMetrics recordRead = hdb.getMetricRecords(queryCondition);

    // THEN
    assertThat(recordRead.getMetrics()).hasSize(2)
      .extracting("metricName")
      .containsOnly("mem_free", "disk_free");

    assertThat(metricsSent.getMetrics())
      .usingElementComparator(TIME_IGNORING_COMPARATOR)
      .containsExactlyElementsOf(recordRead.getMetrics());
  }

  //  @Test
  public void testShouldAggregateMinuteProperly() throws Exception {
    // GIVEN
    TimelineMetricAggregatorMinute aggregatorMinute =
      new TimelineMetricAggregatorMinute(hdb, new Configuration());

    long startTime = System.currentTimeMillis();
    long ctime = startTime;
    long minute = 60 * 1000;
    hdb.insertMetricRecords(prepareTimelineMetrics(startTime, "local"));
    hdb.insertMetricRecords(prepareTimelineMetrics(ctime += minute, "local"));
    hdb.insertMetricRecords(prepareTimelineMetrics(ctime += minute, "local"));
    hdb.insertMetricRecords(prepareTimelineMetrics(ctime += minute, "local"));
    hdb.insertMetricRecords(prepareTimelineMetrics(ctime += minute, "local"));

    // WHEN
    long endTime = startTime + 1000 * 60 * 4;
    boolean success = aggregatorMinute.doWork(startTime, endTime);

    //THEN
    Condition condition = new Condition(null, null, null, null, startTime,
      endTime, null, true);
    condition.setStatement(String.format(GET_METRIC_AGGREGATE_ONLY_SQL,
      METRICS_AGGREGATE_MINUTE_TABLE_NAME));

    PreparedStatement pstmt = PhoenixTransactSQL.prepareGetMetricsSqlStmt
      (conn, condition);
    ResultSet rs = pstmt.executeQuery();
    AbstractTimelineAggregator.MetricHostAggregate expectedAggregate =
      createMetricHostAggregate(2.0, 0.0, 20, 15.0);

    int count = 0;
    while (rs.next()) {
      TimelineMetric currentMetric =
        PhoenixHBaseAccessor.getTimelineMetricKeyFromResultSet(rs);
      AbstractTimelineAggregator.MetricHostAggregate currentHostAggregate =
        PhoenixHBaseAccessor.getMetricHostAggregateFromResultSet(rs);

      if ("disk_free".equals(currentMetric.getMetricName())) {
        assertEquals(2.0, currentHostAggregate.getMax());
        assertEquals(0.0, currentHostAggregate.getMin());
        assertEquals(20, currentHostAggregate.getNumberOfSamples());
        assertEquals(15.0, currentHostAggregate.getSum());
        assertEquals(15.0 / 20, currentHostAggregate.getAvg());
        count++;
      } else if ("mem_free".equals(currentMetric.getMetricName())) {
        assertEquals(2.0, currentHostAggregate.getMax());
        assertEquals(0.0, currentHostAggregate.getMin());
        assertEquals(20, currentHostAggregate.getNumberOfSamples());
        assertEquals(15.0, currentHostAggregate.getSum());
        assertEquals(15.0 / 20, currentHostAggregate.getAvg());
        count++;
      } else {
        fail("Unexpected entry");
      }
    }
    assertEquals("Two aggregated entries expected", 2, count);
  }

  //  @Test
  public void testShouldAggregateHourProperly() throws Exception {
    // GIVEN
    TimelineMetricAggregatorHourly aggregator =
      new TimelineMetricAggregatorHourly(hdb, new Configuration());
    long startTime = System.currentTimeMillis();

    AbstractTimelineAggregator.MetricHostAggregate expectedAggregate =
      createMetricHostAggregate(2.0, 0.0, 20, 15.0);
    Map<TimelineMetric, AbstractTimelineAggregator.MetricHostAggregate>
      aggMap = new HashMap<TimelineMetric,
      AbstractTimelineAggregator.MetricHostAggregate>();

    int min_5 = 5 * 60 * 1000;
    long ctime = startTime - min_5;
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);
    aggMap.put(createEmptyTimelineMetric(ctime += min_5), expectedAggregate);

    hdb.saveHostAggregateRecords(aggMap, METRICS_AGGREGATE_MINUTE_TABLE_NAME);

    //WHEN
    long endTime = ctime + min_5;
    boolean success = aggregator.doWork(startTime, endTime);
    assertTrue(success);

    //THEN
    Condition condition = new Condition(null, null, null, null, startTime,
      endTime, null, true);
    condition.setStatement(String.format(GET_METRIC_AGGREGATE_ONLY_SQL,
      METRICS_AGGREGATE_HOURLY_TABLE_NAME));

    PreparedStatement pstmt = PhoenixTransactSQL.prepareGetMetricsSqlStmt
      (conn, condition);
    ResultSet rs = pstmt.executeQuery();

    while (rs.next()) {
      TimelineMetric currentMetric =
        PhoenixHBaseAccessor.getTimelineMetricKeyFromResultSet(rs);
      AbstractTimelineAggregator.MetricHostAggregate currentHostAggregate =
        PhoenixHBaseAccessor.getMetricHostAggregateFromResultSet(rs);

      if ("disk_used".equals(currentMetric.getMetricName())) {
        assertEquals(2.0, currentHostAggregate.getMax());
        assertEquals(0.0, currentHostAggregate.getMin());
        assertEquals(12 * 20, currentHostAggregate.getNumberOfSamples());
        assertEquals(12 * 15.0, currentHostAggregate.getSum());
        assertEquals(15.0 / 20, currentHostAggregate.getAvg());
      }
    }
  }

  private TimelineMetric createEmptyTimelineMetric(long startTime) {
    TimelineMetric metric = new TimelineMetric();
    metric.setMetricName("disk_used");
    metric.setAppId("test_app");
    metric.setHostName("test_host");
    metric.setTimestamp(startTime);

    return metric;
  }

  private AbstractTimelineAggregator.MetricHostAggregate
  createMetricHostAggregate(double max, double min, int numberOfSamples,
                            double sum) {
    AbstractTimelineAggregator.MetricHostAggregate expectedAggregate =
      new AbstractTimelineAggregator.MetricHostAggregate();
    expectedAggregate.setMax(max);
    expectedAggregate.setMin(min);
    expectedAggregate.setNumberOfSamples(numberOfSamples);
    expectedAggregate.setSum(sum);

    return expectedAggregate;
  }

  private PhoenixHBaseAccessor createTestableHBaseAccessor() {
    return
      new PhoenixHBaseAccessor(
        new Configuration(),
        new Configuration(),
        new ConnectionProvider() {
          @Override
          public Connection getConnection() {
            Connection connection = null;
            try {
              connection = DriverManager.getConnection(getUrl());
            } catch (SQLException e) {
              LOG.warn("Unable to connect to HBase store using Phoenix.", e);
            }
            return connection;
          }
        });
  }

  private final static Comparator<TimelineMetric> TIME_IGNORING_COMPARATOR =
    new Comparator<TimelineMetric>() {
      @Override
      public int compare(TimelineMetric o1, TimelineMetric o2) {
        return o1.equalsExceptTime(o2) ? 0 : 1;
      }
    };

  private TimelineMetrics prepareSingleTimelineMetric(long startTime,
                                                      String host,
                                                      double val) {
    TimelineMetrics m = new TimelineMetrics();
    m.setMetrics(Arrays.asList(
      createTimelineMetric(startTime, "disk_free", host, val)));

    return m;
  }

  private TimelineMetric createTimelineMetric(long startTime,
                                              String metricName,
                                              String host,
                                              double val) {
    TimelineMetric m = new TimelineMetric();
    m.setAppId("host");
    m.setHostName(host);
    m.setMetricName(metricName);
    m.setStartTime(startTime);
    Map<Long, Double> vals = new HashMap<Long, Double>();
    vals.put(startTime + 15000l, val);
    vals.put(startTime + 30000l, val);
    vals.put(startTime + 45000l, val);
    vals.put(startTime + 60000l, val);

    m.setMetricValues(vals);

    return m;
  }

  private TimelineMetrics prepareTimelineMetrics(long startTime, String host) {
    TimelineMetrics metrics = new TimelineMetrics();
    metrics.setMetrics(Arrays.asList(
      createMetric(startTime, "disk_free", host),
      createMetric(startTime, "mem_free", host)));

    return metrics;
  }

  private TimelineMetric createMetric(long startTime,
                                      String metricName,
                                      String host) {
    TimelineMetric m = new TimelineMetric();
    m.setAppId("host");
    m.setHostName(host);
    m.setMetricName(metricName);
    m.setStartTime(startTime);
    Map<Long, Double> vals = new HashMap<Long, Double>();
    vals.put(startTime + 15000l, 0.0);
    vals.put(startTime + 30000l, 0.0);
    vals.put(startTime + 45000l, 1.0);
    vals.put(startTime + 60000l, 2.0);

    m.setMetricValues(vals);

    return m;
  }

  protected static String getUrl() {
    return MY_LOCAL_URL;
//    return  TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL;
  }

  private static Connection getConnection(String url) throws SQLException {
    return DriverManager.getConnection(url);
  }
}