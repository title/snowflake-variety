package example.hc.snowflake_variety.single_datacenter_snowflake;

public class IdGen {

  private volatile boolean enabled;

  private final long timestampBits;
  private final long datacenterIdBits;
  private final long workerIdBits;
  private final long sequenceBits;

  private final long maxDatacenterId;
  private final long maxWorkerId;
  private final long maxSequence;

  private final long timestampShift;
  private final long datacenterIdShift;
  private final long workerIdShift;

  private final long epoch;
  private final long datacenterId;
  private long workerId;

  private long sequence = 0L;
  private long lastTimestamp = -1L;


  public IdGen(IdGenProperties properties) {
    this.timestampBits = properties.getTimestampBits();
    this.datacenterIdBits = properties.getDatacenterIdBits();
    this.workerIdBits = properties.getWorkerIdBits();
    this.sequenceBits = properties.getSequenceBits();
    this.epoch = properties.getEpoch();
    this.datacenterId = properties.getDatacenterId();

    maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
    maxWorkerId = -1L ^ (-1L << workerIdBits);
    maxSequence = -1L ^ (-1L << sequenceBits);

    if (datacenterId > maxDatacenterId || datacenterId < 0) {
      throw new IllegalArgumentException(
          String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
    }

    timestampShift = sequenceBits + datacenterIdBits + workerIdBits;
    datacenterIdShift = sequenceBits + workerIdBits;
    workerIdShift = sequenceBits;

    enabled = false;
  }


  public synchronized long nextId() {
    if (!enabled) {
      throw new IllegalStateException("ID generator is disabled.");
    }

    long currTimestamp = now();

    if (currTimestamp < lastTimestamp) {
      throw new IllegalStateException(
          String.format("Clock moved backwards. Refusing to generate id for %d milliseconds",
              lastTimestamp - currTimestamp));
    }

    if (currTimestamp == lastTimestamp) {
      sequence = (sequence + 1) & maxSequence;
      if (sequence == 0) {
        // overflow: greater than max sequence
        currTimestamp = waitNextMillis(currTimestamp);
      }

    } else {
      // reset for next millisecond
      sequence = 0L;
    }

    lastTimestamp = currTimestamp;

    return ((currTimestamp - epoch) << timestampShift)
        | (datacenterId << datacenterIdShift)
        | (workerId << workerIdShift)
        | sequence;
  }

  private long waitNextMillis(long currTimestamp) {
    while (currTimestamp <= lastTimestamp) {
      currTimestamp = now();
    }
    return currTimestamp;
  }

  private long now() {
    return System.currentTimeMillis();
  }

  public void enable(long workerId) {
    if (workerId > maxWorkerId || workerId < 0) {
      throw new IllegalArgumentException(
          String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
    }

    this.workerId = workerId;
    enabled = true;
  }

  public void disable() {
    enabled = false;
  }
}
