package configProperties

object DB {
  val PathToDatabases = "/tmp"

  val StreamDirName   = "stream"
  val StreamStoreName = "StreamStore"

  val TransactionDataDirName = "transaction_data"

  val TransactionMetaDirName   = "transaction_meta"

  val TransactionMetaStoreName = "TransactionStore"
  val TransactionMetaOpenStoreName = "TransactionOpenStore"

  val TransactionMetaTimeUnit  = java.util.concurrent.TimeUnit.SECONDS
  val TransactionMetaMaxTimeout = 10L

  val ConsumerStoreName = "ConsumerStore"
}
