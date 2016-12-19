package configProperties

object DB {
  val PathToDatabases = configProperties.ServerConfig.dbPath

  val StreamDirName   = "stream"
  val StreamStoreName = "StreamStore"

  val TransactionDataDirName = "transaction_data"

  val TransactionMetaDirName   = "transaction_meta"

  val TransactionMetaStoreName = "TransactionStore"
  val TransactionMetaOpenStoreName = "TransactionOpenStore"

  val ConsumerStoreName = "ConsumerStore"
}
