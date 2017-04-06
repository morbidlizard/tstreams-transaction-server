package ut

import java.io.File
import java.util.concurrent.ArrayBlockingQueue

import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{CommitLogQueueBootstrap, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthOptions, RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class CommitLogQueueBootstrapTestSuite extends FlatSpec with Matchers with BeforeAndAfterAll {
  //arrange
  val authOptions = AuthOptions()
  val rocksStorageOptions = RocksStorageOptions()
  val executionContext = new ServerExecutionContext(2, 1, 1, 1)
  val storageOptions = StorageOptions(new StringBuffer().append("target").append(File.separatorChar).append("clqb").toString)
  val transactionService = new TransactionServer(executionContext, authOptions, storageOptions, rocksStorageOptions)
  val commitLogCatalogue = new CommitLogCatalogue(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory)
  val commitLogQueueBootstrap = new CommitLogQueueBootstrap(10, commitLogCatalogue, transactionService)

  "fillQueue" should "return an empty queue if there are no commit log files in a storage directory" in {
    //act
    val emptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    emptyQueue shouldBe empty
  }

  "fillQueue" should "return a queue of a size that equals to a number of commit log files are in a storage directory" in {
    //arrange
    val numberOfFiles = 10
    createCommitLogFiles(numberOfFiles)

    //act
    val nonemptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    nonemptyQueue should have size numberOfFiles
  }

  "fillQueue" should "return a queue with the time ordered commit log files" in {
    //arrange
    val numberOfFiles = 1
    createCommitLogFiles(numberOfFiles)
    createCommitLogFiles(numberOfFiles)
    createCommitLogFiles(numberOfFiles)

    //act
    val orderedQueue = commitLogQueueBootstrap.fillQueue()
    val orderedFiles = getOrderedFiles(orderedQueue)

    //assert
    orderedFiles shouldBe sorted
  }

  override def afterAll = {
    FileUtils.deleteDirectory(new File(storageOptions.path))
  }

  private def createCommitLogFiles(number: Int) = {
    val commitLogCatalogueByDate = new CommitLogCatalogue(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory)

    (0 until number).foreach(fileNamePrefix => {
      commitLogCatalogueByDate.createFile(fileNamePrefix.toString)
    })
  }

  private def getOrderedFiles(orderedQueue: ArrayBlockingQueue[String]) = {
    val orderedFiles = ArrayBuffer[String]()
    var path = orderedQueue.poll()
    while (path != null) {
      orderedFiles += path
      path = orderedQueue.poll()
    }

    orderedFiles
  }
}
