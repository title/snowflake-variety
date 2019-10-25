package example.hc.snowflake_variety.snowflake_worker_id_with_zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Main {

  public static void main(String[] args) {
    CuratorFramework curator = curatorFramework();
    curator.start();

    IdGen idGen = idGen();
    IdGenCoordinator coordinator = idGenCoordinator(idGen, curator);
    coordinator.init();

    for (int i = 0; i < 20; i++) {
      System.out.println(idGen.nextId());
    }

    curator.close();
  }


  private static CuratorFramework curatorFramework() {
    return CuratorFrameworkFactory.builder()
        .connectString(Config.ZkConnStr)
        .namespace(Config.ZkNamespace)
        .retryPolicy(new ExponentialBackoffRetry(Config.ZkRetryBaseSleepTimeMs, Integer.MAX_VALUE))
        .build();
  }

  private static IdGen idGen() {
    IdGenProperties properties = new IdGenProperties();
    properties.setTimestampBits(Config.TimestampBits);
    properties.setDatacenterIdBits(Config.DatacenterIdBits);
    properties.setWorkerIdBits(Config.WorkerIdBits);
    properties.setSequenceBits(Config.SequenceBits);
    properties.setEpoch(Config.Epoch);
    properties.setDatacenterId(Config.DatacenterId);

    return new IdGen(properties);
  }

  private static IdGenCoordinator idGenCoordinator(IdGen idGen, CuratorFramework curator) {
    long maxWorkerId = (1 << Config.WorkerIdBits) - 1;
    return new IdGenCoordinator(Config.HostIp, curator, idGen, 0, maxWorkerId);
  }
}