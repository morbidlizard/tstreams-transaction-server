package netty

import java.util

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import transactionService.rpc.TransactionService

object Descriptors {

  val protocolFactory = new TBinaryProtocol.Factory

  sealed abstract class Descriptor[T <: ThriftStruct, R <: ThriftStruct](methodName: String, serverCodec: ThriftStructCodec3[T], clientCodec: ThriftStructCodec3[R]) {

    private def encode(entity: ThriftStruct, messageId: Int): Message = {
      val buffer = new TMemoryBuffer(512)
      val oprot = protocolFactory.getProtocol(buffer)

      oprot.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, messageId))
      entity.write(oprot)
      oprot.writeMessageEnd()
      val bytes = util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
      Message(bytes.length, bytes)
    }

    def encodeRequest(entity: T)(implicit messageId: Int): Message = encode(entity, messageId)

    def encodeResponse(entity: R)(implicit messageId: Int): Message = encode(entity, messageId)

    def decodeRequest(message: Message): T = {
      val iprot = protocolFactory.getProtocol(new TMemoryInputTransport(message.body))
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
      val iprot = protocolFactory.getProtocol(new TMemoryInputTransport(message.body))
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

  object Descriptor {
    def decodeMethodName(message: Message): (String, Int) = {
      val iprot  = protocolFactory.getProtocol(new TMemoryInputTransport(message.body))
      val header = iprot.readMessageBegin()
      (header.name, header.seqid)
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

  val methods = Array(
    putStreamMethod, doesStreamExistMethod, getStreamMethod, delStreamMethod,
    putTransactionMethod, putTranscationsMethod, scanTransactionsMethod,
    putTransactionDataMethod, getTransactionDataMethod,
    setConsumerStateMethod, getConsumerStateMethod,
    authenticateMethod, isValidMethod
  )

  case object PutStream extends
    Descriptor(putStreamMethod, TransactionService.PutStream.Args, TransactionService.PutStream.Result)

  case object DoesStreamExist extends
    Descriptor(doesStreamExistMethod, TransactionService.DoesStreamExist.Args, TransactionService.DoesStreamExist.Result)

  case object GetStream extends
    Descriptor(getStreamMethod, TransactionService.GetStream.Args, TransactionService.GetStream.Result)

  case object DelStream extends
    Descriptor(delStreamMethod, TransactionService.DelStream.Args, TransactionService.DelStream.Result)

  case object PutTransaction extends
    Descriptor(putTransactionMethod, TransactionService.PutTransaction.Args, TransactionService.PutTransaction.Result)

  case object PutTransactions extends
    Descriptor(putTranscationsMethod, TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result)

  case object ScanTransactions extends
    Descriptor(scanTransactionsMethod, TransactionService.ScanTransactions.Args, TransactionService.ScanTransactions.Result)

  case object PutTransactionData extends
    Descriptor(putTransactionDataMethod, TransactionService.PutTransactionData.Args, TransactionService.PutTransactionData.Result)

  case object GetTransactionData extends
    Descriptor(getTransactionDataMethod, TransactionService.GetTransactionData.Args, TransactionService.GetTransactionData.Result)

  case object SetConsumerState extends
    Descriptor(setConsumerStateMethod, TransactionService.SetConsumerState.Args, TransactionService.SetConsumerState.Result)

  case object GetConsumerState extends
    Descriptor(getConsumerStateMethod, TransactionService.GetConsumerState.Args, TransactionService.GetConsumerState.Result)

  case object Authenticate extends
    Descriptor(authenticateMethod, TransactionService.Authenticate.Args, TransactionService.Authenticate.Result)

  case object IsValid extends
    Descriptor(isValidMethod, TransactionService.IsValid.Args, TransactionService.IsValid.Result)
}