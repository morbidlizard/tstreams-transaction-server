package transactionService.server.db

import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.rep.{ReplicatedEnvironment, ReplicationConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import configProperties.ServerConfig

object BerkeleyDBReplication {
//  val directory = transactionService.io.FileUtils.dirToFile(resource.DB.PathToDatabases)
//  val environmentConfig = new EnvironmentConfig()
//    .setAllowCreate(true)
//    .setTransactional(true)
//
//  val storeConfig = new StoreConfig()
//    .setAllowCreate(true)
//
//  private val config = new ConfigServer("serverProperties.properties")
//  val replicationConfig = new ReplicationConfig()
//  replicationConfig.setGroupName(config.transactionServerReplicationGroup)
//  replicationConfig.setNodeName(config.transactionServerReplicationName)
//  replicationConfig.setNodeHostPort(config.transactionServerAddress)
//  replicationConfig.setHelperHosts(config.transactionServerEndpoints)
//
//  val environment = new ReplicatedEnvironment(envHome, replicationConfig, environmentConfig)
//
//  val entityStore = new EntityStore(environment, storeName, storeConfig)

}
