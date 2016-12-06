package benchmark

object OneClientOneServerTest extends Installer {
  private val txnCount = 500000
  private val dataSize = 100

  def main(args: Array[String]) {
    clearDB()
    startAuthServer()
    startTransactionServer()
    ClientWriterToOnePartition.main(Array(txnCount.toString, dataSize.toString))
    System.exit(0)
  }
}