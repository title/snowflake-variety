package example.hc.snowflake_variety.snowflake_worker_id_with_zk;

public class IdGenProperties {

  private long timestampBits;
  private long datacenterIdBits;
  private long workerIdBits;
  private long sequenceBits;
  private long epoch;
  private long datacenterId;

  public long getTimestampBits() {
    return timestampBits;
  }

  public void setTimestampBits(long timestampBits) {
    this.timestampBits = timestampBits;
  }

  public long getDatacenterIdBits() {
    return datacenterIdBits;
  }

  public void setDatacenterIdBits(long datacenterIdBits) {
    this.datacenterIdBits = datacenterIdBits;
  }

  public long getWorkerIdBits() {
    return workerIdBits;
  }

  public void setWorkerIdBits(long workerIdBits) {
    this.workerIdBits = workerIdBits;
  }

  public long getSequenceBits() {
    return sequenceBits;
  }

  public void setSequenceBits(long sequenceBits) {
    this.sequenceBits = sequenceBits;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  public long getDatacenterId() {
    return datacenterId;
  }

  public void setDatacenterId(long datacenterId) {
    this.datacenterId = datacenterId;
  }
}
