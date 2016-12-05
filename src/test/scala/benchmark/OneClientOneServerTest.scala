package benchmark

object OneClientOneServerTest extends Installer {
  private val txnCount = 1000000
  private val dataSize = 1000

  def main(args: Array[String]) {
    clearDB()
    startAuthServer()
    startTransactionServer()
    ClientWriterToOnePartition.main(Array(txnCount.toString, dataSize.toString))
    System.exit(0)
  }
}