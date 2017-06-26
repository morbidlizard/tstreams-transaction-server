package ut.multiNodeServer.ZkTreeListTest

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{MetadataRecord, LedgerIDAndItsLastRecordID}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ZkMultipleTreeListReaderMetadataTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
{
  "Metadata record" should "contain timestamp without records" in {
    val timestamp = System.currentTimeMillis()

    val metadataRecord = new MetadataRecord(
      timestamp, Array.empty[LedgerIDAndItsLastRecordID]
    )

    MetadataRecord.fromByteArray(metadataRecord.toByteArray) shouldBe metadataRecord
  }

  it should "contain timestamp with records" in {
    val recordNumber = 10

    val timestamp = System.currentTimeMillis()
    val rand = scala.util.Random
    val records = Array.fill(recordNumber)(
      LedgerIDAndItsLastRecordID(rand.nextLong(), rand.nextLong())
    )

    val metadataRecord = new MetadataRecord(
      timestamp, records
    )

    MetadataRecord.fromByteArray(metadataRecord.toByteArray) shouldBe metadataRecord
  }

}
