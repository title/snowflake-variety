package example.hc.snowflake_variety.snowflake_worker_id_with_zk;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For coordination between different generators, avoiding duplicated worker id
 */
public class IdGenCoordinator {

  private static final Logger logger = LoggerFactory.getLogger(IdGenCoordinator.class);

  private static final Charset Zk_Content_Charset = StandardCharsets.UTF_8;
  private static final String Zk_Path_Root = "/id-gen";
  /**
   * Parent node path for worker ID nodes.<br/>Worker ID nodes, with generator instance ip as
   * content, represent for work IDs already in use.
   */
  private static final String Zk_Path_Worker_Id_List = Zk_Path_Root + "/worker-ids";
  /**
   * Parent node path for IP nodes.<br/>IP nodes, with worker ID as content, represent for
   * generators at work.
   */
  private static final String Zk_Path_Server_Ip_List = Zk_Path_Root + "/gens";
  private static final long Invalid_Worker_Id = -1;

  private final String hostIp;
  private final CuratorFramework curator;
  private final IdGen idGen;
  private final long minWorkerId;
  private final long maxWorkerId;

  private final String ipNodePath;
  private final byte[] ipBytes;


  public IdGenCoordinator(
      String hostIp, CuratorFramework curator, IdGen idGen, long minWorkerId, long maxWorkerId) {
    this.hostIp = hostIp;
    this.curator = curator;
    this.idGen = idGen;
    this.minWorkerId = minWorkerId;
    this.maxWorkerId = maxWorkerId;

    this.ipNodePath = createIpNodePath(hostIp);
    this.ipBytes = serializeContent(hostIp);
  }


  public void init() {
    initZkNode();
    registerConnStateListener();
    enableGenerator();
  }

  /**
   * Init necessary ZooKeeper nodes
   */
  private void initZkNode() {
    try {
      initZkNode(Zk_Path_Worker_Id_List);
      initZkNode(Zk_Path_Server_Ip_List);
    } catch (Exception e) {
      logger.error("Fail to init ZK node for id generator.", e);
      System.exit(-1);
    }
  }

  private void registerConnStateListener() {
    ConnectionStateListener stateListener = (client, newState) -> {
      if (newState == ConnectionState.RECONNECTED) {
        enableGenerator();
      } else {
        disableGenerator();
      }
    };

    curator.getConnectionStateListenable().addListener(stateListener);
  }

  private void enableGenerator() {
    long workerId = getWorkerId();

    if (Invalid_Worker_Id == workerId) {
      logger.error("Fail to enable id generator for invalid worker id.");
      return;
    }

    idGen.enable(workerId);
  }

  private void disableGenerator() {
    idGen.disable();
  }

  private long getWorkerId() {
    try {
      try {
        // Try to get worker ID from ZK node if has already bean registered
        byte[] bytes = curator.getData().forPath(ipNodePath);
        logger.debug("ID generator already registered.");
        return Long.parseLong(deserializeContent(bytes));
      } catch (NoNodeException e) {
        logger.debug("ID generator has not been registered.");
      }

      // Try to register: create worker ID node and IP node in the same transaction
      for (long workerId = minWorkerId; workerId <= maxWorkerId; workerId++) {
        String workerIdNodePath = createWorkerIdNodePath(workerId);
        try {
          CuratorOp createWorkerIdNodeOp = curator.transactionOp()
              .create()
              .withMode(CreateMode.EPHEMERAL)
              .forPath(workerIdNodePath, ipBytes);

          CuratorOp createIpNodeOp = curator.transactionOp()
              .create()
              .withMode(CreateMode.EPHEMERAL)
              .forPath(ipNodePath, serializeContent(Long.toString(workerId)));

          curator.transaction()
              .forOperations(createWorkerIdNodeOp, createIpNodeOp);

          return workerId;
        } catch (NodeExistsException e) {
          if (logger.isDebugEnabled()) {
            logger.debug(String.format("Worker ID %s already in use.", workerId));
          }
        }
      }
    } catch (Exception e) {
      logger.error("Fail to get worker ID for ID generator.", e);
    }

    logger.error("All worker IDs are already in use.");

    return Invalid_Worker_Id;
  }

  private byte[] serializeContent(String content) {
    return content.getBytes(Zk_Content_Charset);
  }

  private String deserializeContent(byte[] content) {
    return new String(content, Zk_Content_Charset);
  }

  private String createWorkerIdNodePath(long workerId) {
    return Zk_Path_Worker_Id_List + "/" + workerId;
  }

  private String createIpNodePath(String ip) {
    return Zk_Path_Server_Ip_List + "/" + ip;
  }

  private void initZkNode(String path) throws Exception {
    try {
      curator.create().creatingParentsIfNeeded().forPath(path);
    } catch (NodeExistsException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("ZK node already exist. " + path);
      }
    }
  }
}
