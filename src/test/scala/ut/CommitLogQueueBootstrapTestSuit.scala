package ut

import java.io.File
import java.util.Date
import java.util.concurrent.ArrayBlockingQueue

import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, CommitLogCatalogueByDate}
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{CommitLogQueueBootstrap, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthOptions, RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class CommitLogQueueBootstrapTestSuit extends FlatSpec with Matchers with BeforeAndAfterAll {
  //arrange
  val authOptions = AuthOptions()
  val rocksStorageOptions = RocksStorageOptions()
  val executionContext = new ServerExecutionContext(2, 1, 1, 1)
  val storageOptions = StorageOptions("target/clqb")
  val transactionService = new TransactionServer(executionContext, authOptions, storageOptions, rocksStorageOptions)
  val commitLogCatalogue = new CommitLogCatalogue(storageOptions.path)
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
    createCommitLogFiles(new Date(640836800000L), numberOfFiles)

    //act
    val nonemptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    nonemptyQueue should have size numberOfFiles
  }

  "fillQueue" should "return a queue with the time ordered commit log files" in {
    //arrange
    val numberOfFiles = 1
    createCommitLogFiles(new Date(641836800000L), numberOfFiles)
    createCommitLogFiles(new Date(642836800000L), numberOfFiles)
    createCommitLogFiles(new Date(643836800000L), numberOfFiles)

    //act
    val orderedQueue = commitLogQueueBootstrap.fillQueue()
    val orderedFiles = getOrderedFiles(orderedQueue)

    //assert
    orderedFiles shouldBe sorted
  }

  override def afterAll = {
    FileUtils.deleteDirectory(new File(storageOptions.path))
  }

  private def createCommitLogFiles(date: Date, number: Int) = {
    commitLogCatalogue.createCatalogue(date)
    val commitLogCatalogueByDate = new CommitLogCatalogueByDate(storageOptions.path, date)

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
