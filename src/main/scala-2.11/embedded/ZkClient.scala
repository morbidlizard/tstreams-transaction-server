package embedded

import org.apache.curator.RetryPolicy
import zooKeeper.ZKLeaderClient

class ZkClient(endpoint: String, sessionTimeoutMillis: Int, connectionTimeoutMillis: Int, policy: RetryPolicy, prefix: String)
  extends ZKLeaderClient(endpoint, sessionTimeoutMillis, connectionTimeoutMillis, policy, prefix)