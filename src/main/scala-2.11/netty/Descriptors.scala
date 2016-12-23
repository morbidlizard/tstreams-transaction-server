package netty

import java.util

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import transactionService.rpc.TransactionService

object Descriptors {

  val protocolFactory = new TBinaryProtocol.Factory

  sealed abstract class Descriptor[T <: ThriftStruct, R <: ThriftStruct](methodName: String, serverCodec: ThriftStructCodec3[T], clientCodec: ThriftStructCodec3[R]) {

    private def encode(entity: ThriftStruct): Array[Byte] = {
      val buffer = new TMemoryBuffer(512)
      val oprot = protocolFactory.getProtocol(buffer)

      oprot.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, 0))
      entity.write(oprot)
      oprot.writeMessageEnd()
      val bytes = util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
      Message(bytes.length, bytes).toByteArray
    }

    def encodeRequest(entity: T): Array[Byte] = encode(entity)

    def encodeResponse(entity: R): Array[Byte] = encode(entity)

    def decodeRequest(bytes: Array[Byte]): (String, T) = {
      val message = Message.fromByteArray(bytes).body
      val iprot = protocolFactory.getProtocol(new TMemoryInputTransport(message))
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

        (msg.name, serverCodec.decode(iprot))
        //         }
      } finally {
        iprot.readMessageEnd()
      }
    }

    def decodeResponse(bytes: Array[Byte]): (String, R) = {
      val message = Message.fromByteArray(bytes).body
      val iprot = protocolFactory.getProtocol(new TMemoryInputTransport(message))
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

        (msg.name, clientCodec.decode(iprot))
        //         }
      } finally {
        iprot.readMessageEnd()
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

  case object PutStream extends
    Descriptor[TransactionService.PutStream.Args, TransactionService.PutStream.Result](putStreamMethod, TransactionService.PutStream.Args, TransactionService.PutStream.Result)

  case object DoesStreamExist extends
    Descriptor[TransactionService.DoesStreamExist.Args, TransactionService.DoesStreamExist.Result](doesStreamExistMethod, TransactionService.DoesStreamExist.Args, TransactionService.DoesStreamExist.Result)

  case object GetStream extends
    Descriptor[TransactionService.GetStream.Args, TransactionService.GetStream.Result](getStreamMethod, TransactionService.GetStream.Args, TransactionService.GetStream.Result)

  case object DelStream extends
    Descriptor[TransactionService.DelStream.Args, TransactionService.DelStream.Result](delStreamMethod, TransactionService.DelStream.Args, TransactionService.DelStream.Result)

  case object PutTransaction extends
    Descriptor[TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result](putTransactionMethod, TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result)

  case object PutTransactions extends
    Descriptor[TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result](putTranscationsMethod, TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result)

  case object ScanTransactions extends
    Descriptor[TransactionService.ScanTransactions.Args, TransactionService.ScanTransactions.Result](scanTransactionsMethod, TransactionService.ScanTransactions.Args, TransactionService.ScanTransactions.Result)

  case object PutTransactionData extends
    Descriptor[TransactionService.PutTransactionData.Args, TransactionService.PutTransactionData.Result](putTransactionDataMethod, TransactionService.PutTransactionData.Args, TransactionService.PutTransactionData.Result)

  case object GetTransactionData extends
    Descriptor[TransactionService.GetTransactionData.Args, TransactionService.GetTransactionData.Result](getTransactionDataMethod, TransactionService.GetTransactionData.Args, TransactionService.GetTransactionData.Result)

  case object SetConsumerState extends
    Descriptor[TransactionService.SetConsumerState.Args, TransactionService.SetConsumerState.Result](setConsumerStateMethod, TransactionService.SetConsumerState.Args, TransactionService.SetConsumerState.Result)

  case object GetConsumerState extends
    Descriptor[TransactionService.GetConsumerState.Args, TransactionService.GetConsumerState.Result](getConsumerStateMethod, TransactionService.GetConsumerState.Args, TransactionService.GetConsumerState.Result)

  case object Authenticate extends
    Descriptor[TransactionService.Authenticate.Args, TransactionService.Authenticate.Result](authenticateMethod, TransactionService.Authenticate.Args, TransactionService.Authenticate.Result)

  case object IsValid extends
    Descriptor[TransactionService.IsValid.Args, TransactionService.IsValid.Result](isValidMethod, TransactionService.IsValid.Args, TransactionService.IsValid.Result)
}