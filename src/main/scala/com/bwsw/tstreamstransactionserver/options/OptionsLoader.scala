package com.bwsw.tstreamstransactionserver.options

import java.io.FileInputStream
import java.util.Properties

import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import org.rocksdb.CompressionType

class OptionsLoader() {
  require(System.getProperty(CommonOptions.propertyFileName) != null,
    s"There is no file with properties. You should define a path to a property file through '-D${CommonOptions.propertyFileName}=<path_to_file>' " +
      s"(e.g. 'java -D${CommonOptions.propertyFileName}=/home/user/config.properties " +
      "-cp target/scala-2.12/tstreams-transaction-server-1.3.7.4-SNAPSHOT.jar:/home/user/slf4j-api-1.7.24.jar:/home/user/slf4j-simple-1.7.24.jar " +
      "com.bwsw.tstreamstransactionserver.ServerLauncher').")

  private val props = new Properties()
  props.load(new FileInputStream(System.getProperty(CommonOptions.propertyFileName)))

  private val serverAuthOptions = loadServerAuthOptions()
  private val zookeeperOptions = loadZookeeperOptions()
  private val bootstrapOptions = loadBootstrapOptions()
  private val serverReplicationOptions = loadServerReplicationOptions()
  private val serverStorageOptions = loadServerStorageOptions()
  private val serverRocksStorageOptions = loadServerRocksStorageOptions()
  private val packageTransmissionOptions = loadPackageTransmissionOptions()
  private val commitLogOptions = loadCommitLogOptions()
  private val zookeeperSpecificOptions = loadZookeeperSpecificOptions()
  private val subscribersUpdateOptions = loadSubscribersUpdateOptions()


  private def loadBootstrapOptions() = {
    val fields = getPropertiesOf(classOf[BootstrapOptions])

    castCheck(BootstrapOptions(fields(0), fields(1).toInt, fields(2).toInt))
  }

  private def loadSubscribersUpdateOptions() = {
    val fields = getPropertiesOf(classOf[SubscriberUpdateOptions])

    castCheck(SubscriberUpdateOptions(fields(0).toInt))
  }

  private def loadServerAuthOptions() = {
    val fields = getPropertiesOf(classOf[ServerOptions.AuthOptions])

    castCheck(ServerOptions.AuthOptions(fields(0), fields(1).toInt, fields(2).toInt))
  }

  private def loadServerStorageOptions() = {
    val fields = getPropertiesOf(classOf[StorageOptions])

    castCheck(StorageOptions(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5)))
  }

  private def loadServerReplicationOptions() = {
    val fields = getPropertiesOf(classOf[ServerReplicationOptions])

    castCheck(ServerReplicationOptions(fields(0), fields(1), fields(2), fields(3)))
  }

  private def loadServerRocksStorageOptions() = {
    val fields = getPropertiesOf(classOf[RocksStorageOptions])

    castCheck(RocksStorageOptions(fields(0).toInt, fields(1).toInt, fields(2).toInt,
      fields(3).toInt, fields(4).toInt, CompressionType.getCompressionType(fields(5)), fields(6).toBoolean))
  }

  private def loadZookeeperOptions() = {
    val fields = getPropertiesOf(classOf[ZookeeperOptions], "zk.")

    castCheck(ZookeeperOptions(fields(0), fields(1), fields(2).toInt, fields(3).toInt, fields(4).toInt))
  }

  private def loadPackageTransmissionOptions() = {
    val fields = getPropertiesOf(classOf[TransportOptions])

    castCheck(TransportOptions(fields(0).toInt, fields(1).toInt))
  }

  private def loadCommitLogOptions() = {
    val fields = getPropertiesOf(classOf[CommitLogOptions])

    castCheck(CommitLogOptions(CommitLogWriteSyncPolicy.withName(fields(0)), fields(1).toInt, IncompleteCommitLogReadPolicy.withName(fields(2)), fields(3).toInt, fields(4).toInt))
  }

  private def loadZookeeperSpecificOptions() = {
    val fields = getPropertiesOf(classOf[ServerOptions.ZooKeeperOptions])

    castCheck(ServerOptions.ZooKeeperOptions(fields(0)))
  }

  private def getPropertiesOf(_class: Class[_], prefix: String = "") = {
    def getProperty(name: String) = {
      val propertyName = createPropertyName(name, prefix)

      Option(props.getProperty(propertyName)) match {
        case Some(property) => property
        case None => throw new NoSuchElementException(s"No property by key: '$propertyName' has been found for '${_class.getSimpleName}'. " +
          s"You should define it and restart the program.")
      }
    }

    def createPropertyName(name: String, prefix: String) = {
      prefix + (if (name.toLowerCase != name) name.replaceAll("([A-Z])", ".$1").toLowerCase() else name)
    }

    _class.getDeclaredFields.map(_.getName).map(getProperty)
  }

  private def castCheck[T](constructor: => T): T = {
    try {
      constructor
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Some property(ies) has got an invalid format (inconsistency between types).")
    }
  }

  def getServerAuthOptions = {
    serverAuthOptions
  }

  def getZookeeperOptions = {
    zookeeperOptions
  }

  def getBootstrapOptions = {
    bootstrapOptions
  }

  def getServerReplicationOptions = {
    serverReplicationOptions
  }

  def getServerStorageOptions = {
    serverStorageOptions
  }

  def getServerRocksStorageOptions = {
    serverRocksStorageOptions
  }

  def getPackageTransmissionOptions = {
    packageTransmissionOptions
  }

  def getCommitLogOptions = {
    commitLogOptions
  }

  def getZookeeperSpecificOptions = {
    zookeeperSpecificOptions
  }

  def getSubscribersUpdateOptions = {
    subscribersUpdateOptions
  }
}