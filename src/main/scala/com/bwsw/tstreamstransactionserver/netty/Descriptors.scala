package com.bwsw.tstreamstransactionserver.netty

import java.util

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}
import org.apache.thrift.protocol._
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import com.bwsw.tstreamstransactionserver.rpc.TransactionService

object Descriptors {


  /** A class for building Descriptors objects that contain all necessary information how to serialize/deserialize structures of request/response methods,
    * how request and response are connected with each other.
    *
    *  @constructor create a new descriptor that could serialize/deserialize structures of methods.
    *  @param methodName name of a method. All methods names should be distinct in all Descriptor objects.
    *  @param codecReq a request type to serialize/deserialize.
    *  @param codecRep a response type to serialize/deserialize.
    *  @param protocolReq a protocol for serialization/deserialization of method arguments of request.
    *  @param protocolRep a protocol for serialization/deserialization of method arguments of response.
    */
  sealed abstract class Descriptor[Request <: ThriftStruct, Response <: ThriftStruct](methodName: String,
                                                                         codecReq: ThriftStructCodec3[Request],
                                                                         codecRep: ThriftStructCodec3[Response],
                                                                         protocolReq : TProtocolFactory,
                                                                         protocolRep : TProtocolFactory) {




    /** A method for building request/response methods to serialize.
      *
      *  @param entity name of a method. All methods names should be distinct in all Descriptor objects.
      *  @param protocol a protocol for serialization/deserialization of method.
      *  @param messageId an id of serialized instance of request/response.
      *  @return a new message containing binary representation of method's object, it's size and the protocol to
      *          serialize/deserialize the method.
      *
      *
      */
    private final def encode(entity: ThriftStruct, protocol: TProtocolFactory, messageId: Int, token: Int): Message = {
      val buffer = new TMemoryBuffer(512)
      val oprot = protocol.getProtocol(buffer)

      oprot.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, messageId))
      entity.write(oprot)
      oprot.writeMessageEnd()
      val bytes = util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
      Message(bytes.length, getProtocolID(protocol), bytes, token)
    }

    /** A method for serializing request and adding an id to id. */
    final def encodeRequest(entity: Request)(messageId: Int, token: Int): Message = {
      encode(entity, protocolReq, messageId, token)
    }

    /** A method for serializing response and adding an id to id. */
    final def encodeResponse(entity: Response)(messageId: Int, token: Int): Message = encode(entity, protocolRep, messageId, token)


    /** A method for deserialization request.
      *
      *  @param message a structure that contains a binary body of request.
      *  @return a request
      */
    final def decodeRequest(message: Message): Request = {
      val iprot = protocolReq.getProtocol(new TMemoryInputTransport(message.body))
      val msg = iprot.readMessageBegin()
      try {
        //         if (msg.`type` == TMessageType.EXCEPTION) {
        //           val com.bwsw.exception = TApplicationException.read(iprot) match {
        //             case sourced: SourcedException =>
        //               if (serviceName != "") sourced.serviceName = serviceName
        //               sourced
        //             case e => e
        //           }
        //           throw com.bwsw.exception
        //         } else {

        codecReq.decode(iprot)
        //         }
      } finally {
        iprot.readMessageEnd()
      }
    }

    /** A method for deserialization response.
      *
      *  @param message a structure that contains a binary body of response.
      *  @return a response
      */
    final def decodeResponse(message: Message): Response = {
      val iprot = protocolRep.getProtocol(new TMemoryInputTransport(message.body))
      val msg = iprot.readMessageBegin()
      try {
        //         if (msg.`type` == TMessageType.EXCEPTION) {
        //           val com.bwsw.exception = TApplicationException.read(iprot) match {
        //             case sourced: SourcedException =>
        //               if (serviceName != "") sourced.serviceName = serviceName
        //               sourced
        //             case e => e
        //           }
        //           throw com.bwsw.exception
        //         } else {

        codecRep.decode(iprot)
        //         }
      } finally {
        iprot.readMessageEnd()
      }
    }
  }

  object Descriptor {
    /** A method for deserialization response/request header.
      *
      *  @param message a structure that contains a binary body of response/request.
      *  @return a method name and it's id
      */
    def decodeMethodName(message: Message): (String, Int) = {
      val iprot = getIdProtocol(message.protocol).getProtocol(new TMemoryInputTransport(message.body))
      val header = iprot.readMessageBegin()
      (header.name, header.seqid)
    }
  }

  private val protocolTCompactFactory = new TCompactProtocol.Factory
  private val protocolTBinaryFactory = new TBinaryProtocol.Factory

  /** get byte by protocol  */
  def getProtocolID(protocol: TProtocolFactory): Byte = protocol match {
    case `protocolTCompactFactory` => 0
    case `protocolTBinaryFactory`  => 1
  }

  /** get protocol by byte  */
  def getIdProtocol(byte: Byte): TProtocolFactory = byte match {
    case 0 => protocolTCompactFactory
    case 1 => protocolTBinaryFactory
  }

  /** All methods names should be unique */
  val getCommitLogOffsetsMethod = "getCommitLogOffsets"
  val putStreamMethod = "putStream"
  val checkStreamExists = "checkStreamExist"
  val getStreamMethod = "getStream"
  val delStreamMethod = "delStream"
  val putTransactionMethod = "putTransaction"
  val putSimpleTransactionAndDataMethod = "putSimpleTransactionAndData"
  val putTransactionsMethod = "putTransactions"
  val getTransactionMethod = "getTransaction"
  val getLastCheckpointedTransactionMethod = "getLastCheckpointedTransaction"
  val scanTransactionsMethod = "scanTransactions"
  val putTransactionDataMethod = "putTransactionData"
  val getTransactionDataMethod = "getTransactionData"
  val putConsumerCheckpointMethod = "putConsumerCheckpoint"
  val getConsumerStateMethod = "getConsumerState"
  val authenticateMethod = "authenticate"
  val isValidMethod = "isValid"

  final def methodWithArgsToString(id: Int, struct: ThriftStruct): String = {
    def toString(methodName: String, arguments: Iterator[Any], fields: List[String]) = {
      val argumentsList = arguments.toList
      fields zip argumentsList mkString(s"request id $id - $methodName: ", " ", "")
    }
    struct match {
      case struct: TransactionService.GetCommitLogOffsets.Args  => toString(getCommitLogOffsetsMethod, struct.productIterator, TransactionService.GetCommitLogOffsets.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutStream.Args         => toString(putStreamMethod, struct.productIterator, TransactionService.PutStream.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.CheckStreamExists.Args => toString(checkStreamExists, struct.productIterator, TransactionService.CheckStreamExists.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetStream.Args         => toString(getStreamMethod, struct.productIterator, TransactionService.GetStream.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.DelStream.Args         => toString(delStreamMethod, struct.productIterator, TransactionService.DelStream.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutTransaction.Args    => toString(putTransactionMethod, struct.productIterator, TransactionService.PutTransaction.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutSimpleTransactionAndData.Args => toString(putSimpleTransactionAndDataMethod, struct.productIterator, TransactionService.PutSimpleTransactionAndData.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutTransactions.Args   => toString(putTransactionsMethod, struct.productIterator, TransactionService.PutTransactions.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetTransaction.Args    => toString(getTransactionMethod, struct.productIterator, TransactionService.GetTransaction.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetLastCheckpointedTransaction.Args => toString(getLastCheckpointedTransactionMethod, struct.productIterator, TransactionService.GetLastCheckpointedTransaction.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.ScanTransactions.Args   => toString(scanTransactionsMethod, struct.productIterator, TransactionService.ScanTransactions.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutTransactionData.Args  => toString(putTransactionDataMethod, struct.productIterator, TransactionService.PutTransactionData.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetTransactionData.Args  => toString(getTransactionDataMethod, struct.productIterator, TransactionService.GetTransactionData.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutConsumerCheckpoint.Args => toString(putConsumerCheckpointMethod, struct.productIterator, TransactionService.PutConsumerCheckpoint.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetConsumerState.Args => toString(getConsumerStateMethod, struct.productIterator, TransactionService.GetConsumerState.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.Authenticate.Args   => toString(authenticateMethod, struct.productIterator, TransactionService.Authenticate.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.IsValid.Args   => toString(isValidMethod, struct.productIterator, TransactionService.IsValid.Args.fieldInfos.map(_.tfield.name))
      case struct => throw new NotImplementedError(s"$struct is not implemeted for debug information")
    }
  }

  case object GetCommitLogOffsets extends
    Descriptor(getCommitLogOffsetsMethod, TransactionService.GetCommitLogOffsets.Args, TransactionService.GetCommitLogOffsets.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object PutStream extends
    Descriptor(putStreamMethod, TransactionService.PutStream.Args, TransactionService.PutStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object CheckStreamExists extends
    Descriptor(checkStreamExists, TransactionService.CheckStreamExists.Args, TransactionService.CheckStreamExists.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object GetStream extends
    Descriptor(getStreamMethod, TransactionService.GetStream.Args, TransactionService.GetStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object DelStream extends
    Descriptor(delStreamMethod, TransactionService.DelStream.Args, TransactionService.DelStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object PutTransaction extends
    Descriptor(putTransactionMethod, TransactionService.PutTransaction.Args, TransactionService.PutTransaction.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object PutTransactions extends
    Descriptor(putTransactionsMethod, TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result, protocolTCompactFactory, protocolTBinaryFactory)

  case object PutSimpleTransactionAndData extends
    Descriptor(putSimpleTransactionAndDataMethod, TransactionService.PutSimpleTransactionAndData.Args, TransactionService.PutSimpleTransactionAndData.Result, protocolTCompactFactory, protocolTBinaryFactory)

  case object GetTransaction extends
    Descriptor(getTransactionMethod, TransactionService.GetTransaction.Args, TransactionService.GetTransaction.Result, protocolTBinaryFactory, protocolTCompactFactory)

  case object GetLastCheckpointedTransaction extends
    Descriptor(getLastCheckpointedTransactionMethod, TransactionService.GetLastCheckpointedTransaction.Args, TransactionService.GetLastCheckpointedTransaction.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object ScanTransactions extends
    Descriptor(scanTransactionsMethod, TransactionService.ScanTransactions.Args, TransactionService.ScanTransactions.Result, protocolTBinaryFactory, protocolTCompactFactory)

  case object PutTransactionData extends
    Descriptor(putTransactionDataMethod, TransactionService.PutTransactionData.Args, TransactionService.PutTransactionData.Result, protocolTCompactFactory, protocolTBinaryFactory)

  case object GetTransactionData extends
    Descriptor(getTransactionDataMethod, TransactionService.GetTransactionData.Args, TransactionService.GetTransactionData.Result, protocolTBinaryFactory, protocolTCompactFactory)

  case object PutConsumerCheckpoint extends
    Descriptor(putConsumerCheckpointMethod, TransactionService.PutConsumerCheckpoint.Args, TransactionService.PutConsumerCheckpoint.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object GetConsumerState extends
    Descriptor(getConsumerStateMethod, TransactionService.GetConsumerState.Args, TransactionService.GetConsumerState.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object Authenticate extends
    Descriptor(authenticateMethod, TransactionService.Authenticate.Args, TransactionService.Authenticate.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object IsValid extends
    Descriptor(isValidMethod, TransactionService.IsValid.Args, TransactionService.IsValid.Result, protocolTBinaryFactory, protocolTBinaryFactory)
}