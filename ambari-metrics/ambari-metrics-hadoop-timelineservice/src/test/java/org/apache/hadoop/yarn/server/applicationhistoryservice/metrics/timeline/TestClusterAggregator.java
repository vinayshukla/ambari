package org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.*;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.AbstractTimelineAggregator.MetricClusterAggregate;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.AbstractTimelineAggregator.MetricHostAggregate;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.PhoenixTransactSQL.*;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.TimelineMetricClusterAggregator.TimelineClusterMetric;
import static org.assertj.core.api.Assertions.assertThat;

public class TestClusterAggregator {
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

  @Test
  public void testShouldAggregateClusterProperly() throws Exception {
    // GIVEN
    TimelineMetricClusterAggregator agg =
      new TimelineMetricClusterAggregator(hdb, new Configuration());

    long startTime = System.currentTimeMillis();
    long ctime = startTime;
    long minute = 60 * 1000;
    hdb.insertMetricRecords(prepareSingleTimelineMetric(ctime, "local1", 1));
    hdb.insertMetricRecords(prepareSingleTimelineMetric(ctime, "local2", 2));
    ctime += minute;
    hdb.insertMetricRecords(prepareSingleTimelineMetric(ctime, "local1", 2));
    hdb.insertMetricRecords(prepareSingleTimelineMetric(ctime, "local2", 1));

    // WHEN
    long endTime = ctime + minute;
    boolean success = agg.doWork(startTime, endTime);

    //THEN
    Condition condition = new Condition(null, null, null, null, startTime,
      endTime, null, true);
    condition.setStatement(GET_CLUSTER_AGGREGATE_SQL);

    PreparedStatement pstmt = PhoenixTransactSQL.prepareGetMetricsSqlStmt
      (conn, condition);
    ResultSet rs = pstmt.executeQuery();
    MetricHostAggregate expectedAggregate =
      createMetricHostAggregate(2.0, 0.0, 20, 15.0);

    int recordCount = 0;
    while (rs.next()) {
      TimelineClusterMetric currentMetric =
        PhoenixHBaseAccessor.getTimelineMetricClusterKeyFromResultSet(rs);
      MetricClusterAggregate currentHostAggregate =
        PhoenixHBaseAccessor.getMetricClusterAggregateFromResultSet(rs);

      if ("disk_free".equals(currentMetric.getMetricName())) {
        assertEquals(2, currentHostAggregate.getNumberOfHosts());
        assertEquals(2.0, currentHostAggregate.getMax());
        assertEquals(1.0, currentHostAggregate.getMin());
        assertEquals(3.0, currentHostAggregate.getSum());
        recordCount++;
      } else {
        fail("Unexpected entry");
      }
    }
  }

  @Test
  public void testShouldAggregateClusterOnHourProperly() throws Exception {
    // GIVEN
    TimelineMetricClusterAggregatorHourly agg =
      new TimelineMetricClusterAggregatorHourly(hdb, new Configuration());

    // this time can be virtualized! or made independent from real clock
    long startTime = System.currentTimeMillis();
    long ctime = startTime;
    long minute = 60 * 1000;

    Map<TimelineClusterMetric, MetricClusterAggregate> records =
      new HashMap<TimelineClusterMetric, MetricClusterAggregate>();

    records.put(createEmptyTimelineMetric(ctime),
      new MetricClusterAggregate(4.0, 2, 0.0, 4.0, 0.0));
    records.put(createEmptyTimelineMetric(ctime += minute),
      new MetricClusterAggregate(4.0, 2, 0.0, 4.0, 0.0));
    records.put(createEmptyTimelineMetric(ctime += minute),
      new MetricClusterAggregate(4.0, 2, 0.0, 4.0, 0.0));
    records.put(createEmptyTimelineMetric(ctime += minute),
      new MetricClusterAggregate(4.0, 2, 0.0, 4.0, 0.0));

    hdb.saveClusterAggregateRecords(records);

    // WHEN
    agg.doWork(startTime, ctime + minute);

    // THEN
    ResultSet rs = executeQuery("SELECT * FROM METRIC_AGGREGATE_HOURLY");
    int count = 0;
    while (rs.next()) {
      assertEquals("METRIC_NAME", "disk_used", rs.getString("METRIC_NAME"));
      assertEquals("APP_ID", "test_app", rs.getString("APP_ID"));
      assertEquals("METRIC_SUM", 16.0, rs.getDouble("METRIC_SUM"));
      assertEquals("METRIC_COUNT", 8, rs.getLong("METRIC_COUNT"));
      assertEquals("METRIC_MAX", 4.0, rs.getDouble("METRIC_MAX"));
      assertEquals("METRIC_MIN", 0.0, rs.getDouble("METRIC_MIN"));
      count++;
    }

    assertEquals("One hourly aggregated row expected ",1, count);

    System.out.println(rs);


  }

  private ResultSet executeQuery(String query) throws SQLException {
    Connection conn = getConnection(getUrl());
    Statement stmt = conn.createStatement();
    return stmt.executeQuery(query);
  }

  private TimelineClusterMetric createEmptyTimelineMetric(long startTime) {
    TimelineClusterMetric metric = new TimelineClusterMetric("disk_used",
      "test_app", null, startTime, null);

    return metric;
  }

  private MetricHostAggregate
  createMetricHostAggregate(double max, double min, int numberOfSamples,
                            double sum) {
    MetricHostAggregate expectedAggregate =
      new MetricHostAggregate();
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


  protected static String getUrl() {
    return MY_LOCAL_URL;
//    return  TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL;
  }

  private static Connection getConnection(String url) throws SQLException {
    return DriverManager.getConnection(url);
  }
}