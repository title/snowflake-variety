package example.hc.snowflake_variety.snowflake_worker_id_with_zk;

public abstract class Config {

  public static final long TimestampBits = 41L;
  public static final long DatacenterIdBits = 5L;
  public static final long WorkerIdBits = 5L;
  public static final long SequenceBits = 12L;

  /**
   * Epoch for id generator。Unit is milliseconds. Can not be modified after initialization.
   * 1546300800000 means 2019-01-01 UTC
   */
  public static final long Epoch = 1546300800000L;

  /**
   * datacenter id should be configured in advance，can not be changed at runtime
   */
  public static long DatacenterId = 0;

  // For Coordinator

  public static String HostIp = "10.1.100.111";
  public static String ZkConnStr = "10.1.100.100:2181";
  public static String ZkNamespace = "example";
  public static int ZkRetryBaseSleepTimeMs = 1000;
}
