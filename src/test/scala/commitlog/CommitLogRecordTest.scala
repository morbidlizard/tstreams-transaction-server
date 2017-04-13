package commitlog

import com.bwsw.commitlog.CommitLogRecord
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CommitLogRecordTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  "CommitLogRecord" should "be serialized/deserialized" in {
    val record1 = new CommitLogRecord(1L, 0:Byte, "test_data".getBytes())
    CommitLogRecord.fromByteArrayWithDelimiter(record1.toByteArrayWithDelimiter).right.get shouldBe record1

    val record2 = new CommitLogRecord(-3L, -5:Byte, "".getBytes())
    CommitLogRecord.fromByteArrayWithDelimiter(record2.toByteArrayWithDelimiter).right.get shouldBe record2
  }

}
