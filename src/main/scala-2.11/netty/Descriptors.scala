package netty

import java.util

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}
import org.apache.thrift.protocol._
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport, TTransport}
import transactionService.rpc.TransactionService

object Descriptors {
  sealed abstract class Descriptor[T <: ThriftStruct, R <: ThriftStruct](methodName: String,
                                                                         serverCodec: ThriftStructCodec3[T],
                                                                         clientCodec: ThriftStructCodec3[R],
                                                                         protocolReq : TProtocolFactory,
                                                                         protocolRep : TProtocolFactory) {

    private def encode(entity: ThriftStruct, protocol: TProtocolFactory ,messageId: Int): Message = {
      val buffer = new TMemoryBuffer(512)
      val oprot = protocol.getProtocol(buffer)

      oprot.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, messageId))
      entity.write(oprot)
      oprot.writeMessageEnd()
      val bytes = util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
      Message(bytes.length, bytes)
    }

    def encodeRequest(entity: T)(messageId: Int): Message = encode(entity, protocolReq, messageId)

    def encodeResponse(entity: R)(messageId: Int): Message = encode(entity, protocolRep, messageId)

    def decodeRequest(message: Message): T = {
      val iprot = protocolReq.getProtocol(new TMemoryInputTransport(message.body))
      val msg = iprot.readMessageBegin()
      try {
        //         if (msg.`type` == TMessageType.EXCEPTION) {
        //           val exception = TApplicationException.read(iprot) match {
        //             case sourced: SourcedException =>
        //               if (serviceName != "") sourced.serviceName = serviceName
        //               sourced
        //             case e => e
        //           }
        //           throw exception
        //         } else {

        serverCodec.decode(iprot)
        //         }
      } finally {
        iprot.readMessageEnd()
      }
    }

    def decodeResponse(message: Message): R = {
      val iprot = protocolRep.getProtocol(new TMemoryInputTransport(message.body))
      val msg = iprot.readMessageBegin()
      try {
        //         if (msg.`type` == TMessageType.EXCEPTION) {
        //           val exception = TApplicationException.read(iprot) match {
        //             case sourced: SourcedException =>
        //               if (serviceName != "") sourced.serviceName = serviceName
        //               sourced
        //             case e => e
        //           }
        //           throw exception
        //         } else {

        clientCodec.decode(iprot)
        //         }
      } finally {
        iprot.readMessageEnd()
      }
    }
  }

  private val protocolTCompactFactory = new TCompactProtocol.Factory
  private val protocolTBinaryFactory = new TBinaryProtocol.Factory
  object Descriptor {
    def decodeMethodName(message: Message): (String, Int) = {
      scala.util.Try {
        val iprot = protocolTBinaryFactory.getProtocol(new TMemoryInputTransport(message.body))
        val header = iprot.readMessageBegin()
        (header.name, header.seqid)
      } match {
        case scala.util.Success(result) => result
        case _ =>
          val iprot = protocolTCompactFactory.getProtocol(new TMemoryInputTransport(message.body))
          val header = iprot.readMessageBegin()
          (header.name, header.seqid)
      }
    }
  }

  val putStreamMethod = "putStream"
  val doesStreamExistMethod = "doesStreamExist"
  val getStreamMethod = "getStream"
  val delStreamMethod = "delStream"
  val putTransactionMethod = "putTransaction"
  val putTranscationsMethod = "putTransactions"
  val scanTransactionsMethod = "scanTransactions"
  val putTransactionDataMethod = "putTransactionData"
  val getTransactionDataMethod = "getTransactionData"
  val setConsumerStateMethod = "setConsumerState"
  val getConsumerStateMethod = "getConsumerState"
  val authenticateMethod = "authenticate"
  val isValidMethod = "isValid"
  val tokenInvalidException ="tokenInvalidException"

  val methods = Array(
    putStreamMethod, doesStreamExistMethod, getStreamMethod, delStreamMethod,
    putTransactionMethod, putTranscationsMethod, scanTransactionsMethod,
    putTransactionDataMethod, getTransactionDataMethod,
    setConsumerStateMethod, getConsumerStateMethod,
    authenticateMethod, isValidMethod
  )


  case object PutStream extends
    Descriptor(putStreamMethod, TransactionService.PutStream.Args, TransactionService.PutStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object DoesStreamExist extends
    Descriptor(doesStreamExistMethod, TransactionService.DoesStreamExist.Args, TransactionService.DoesStreamExist.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object GetStream extends
    Descriptor(getStreamMethod, TransactionService.GetStream.Args, TransactionService.GetStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object DelStream extends
    Descriptor(delStreamMethod, TransactionService.DelStream.Args, TransactionService.DelStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object PutTransaction extends
    Descriptor(putTransactionMethod, TransactionService.PutTransaction.Args, TransactionService.PutTransaction.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object PutTransactions extends
    Descriptor(putTranscationsMethod, TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result, protocolTCompactFactory, protocolTBinaryFactory)

  case object ScanTransactions extends
    Descriptor(scanTransactionsMethod, TransactionService.ScanTransactions.Args, TransactionService.ScanTransactions.Result, protocolTBinaryFactory, protocolTCompactFactory)

  case object PutTransactionData extends
    Descriptor(putTransactionDataMethod, TransactionService.PutTransactionData.Args, TransactionService.PutTransactionData.Result, protocolTCompactFactory, protocolTBinaryFactory)

  case object GetTransactionData extends
    Descriptor(getTransactionDataMethod, TransactionService.GetTransactionData.Args, TransactionService.GetTransactionData.Result, protocolTBinaryFactory, protocolTCompactFactory)

  case object SetConsumerState extends
    Descriptor(setConsumerStateMethod, TransactionService.SetConsumerState.Args, TransactionService.SetConsumerState.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object GetConsumerState extends
    Descriptor(getConsumerStateMethod, TransactionService.GetConsumerState.Args, TransactionService.GetConsumerState.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object Authenticate extends
    Descriptor(authenticateMethod, TransactionService.Authenticate.Args, TransactionService.Authenticate.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object IsValid extends
    Descriptor(isValidMethod, TransactionService.IsValid.Args, TransactionService.IsValid.Result, protocolTBinaryFactory, protocolTBinaryFactory)
}