package configProperties

object DB {
  val PathToDatabases = "/tmp"

  val StreamDirName   = "stream"
  val StreamStoreName = "StreamStore"

  val TransactionDataDirName = "transaction_data"

  val TransactionMetaDirName   = "transaction_meta"
  val TransactionMetaStoreName = "TransactionStore"
  val TransactionMetaProducerSecondaryIndexName = "stateDB"
  val TransactionMetaTimeUnit  = java.util.concurrent.TimeUnit.MINUTES
  val TransactionMetaMaxTimeout = 5L

  val ConsumerStoreName = "ConsumerStore"
}
