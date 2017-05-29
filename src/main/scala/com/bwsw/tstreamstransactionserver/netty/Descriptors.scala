package com.bwsw.tstreamstransactionserver.netty

import java.util

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}
import org.apache.thrift.protocol._
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import com.bwsw.tstreamstransactionserver.rpc.{ServerException, TransactionService}

import scala.collection.mutable

object Descriptors {


  /** A class for building Descriptors objects that contain all necessary information how to serialize/deserialize structures of request/response methods,
    * how request and response are connected with each other.
    *
    *  @constructor create a new descriptor that could serialize/deserialize structures of methods.
    *  @param name name of a method. All methods names should be distinct in all Descriptor objects.
    *  @param methodID a method ID.
    *  @param codecReq a request type to serialize/deserialize.
    *  @param codecRep a response type to serialize/deserialize.
    *  @param protocolReq a protocol for serialization/deserialization of method arguments of request.
    *  @param protocolRep a protocol for serialization/deserialization of method arguments of response.
    */
  sealed abstract class Descriptor[Request <: ThriftStruct, Response <: ThriftStruct](val name: String,
                                                                                      val methodID: Byte,
                                                                                      codecReq: ThriftStructCodec3[Request],
                                                                                      codecRep: ThriftStructCodec3[Response],
                                                                                      protocolReq : TProtocolFactory,
                                                                                      protocolRep : TProtocolFactory)
  extends Product
    with Serializable
  {

    /** A method for building request/response methods to serialize.
      *
      * @param entity    name of a method. All methods names should be distinct in all Descriptor objects.
      * @param protocol  a protocol for serialization/deserialization of method.
      * @param messageId an id of serialized instance of request/response.
      * @return a new message containing binary representation of method's object, it's size and the protocol to
      *         serialize/deserialize the method.
      *
      *
      */
    @inline
    private final def encode(entity: ThriftStruct, protocol: TProtocolFactory, messageId: Long, token: Int, isFireAndForgetMethod: Boolean): Message = {
      val buffer = new TMemoryBuffer(128)
      val oprot = protocol.getProtocol(buffer)

      entity.write(oprot)

      val bytes = util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
      val isFireAndForgetMethodToByte = if (isFireAndForgetMethod) 1:Byte else 0:Byte
      Message(messageId, bytes.length, getProtocolID(protocol), bytes, token, methodID, isFireAndForgetMethodToByte)
    }

    /** A method for serializing request and adding an id to id. */
    @inline
    final def encodeRequestToMessage(entity: Request)(messageId: Long, token: Int, isFireAndForgetMethod: Boolean): Message =
      encode(entity, protocolReq, messageId, token, isFireAndForgetMethod)


    @inline
    final def encodeRequest(entity: Request): Array[Byte] = {
      val buffer = new TMemoryBuffer(128)
      val oprot =  protocolReq.getProtocol(buffer)

      entity.write(oprot)

      util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
    }

    @inline
    final def encodeResponse(entity: Response): Array[Byte] = {
      val buffer = new TMemoryBuffer(128)
      val oprot =  protocolRep.getProtocol(buffer)

      entity.write(oprot)

      util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
    }

    /** A method for serializing response and adding an id to id. */
    @inline
    final def encodeResponseToMessage(entity: Response)(messageId: Long, token: Int, isFireAndForgetMethod: Boolean): Message =
    encode(entity, protocolRep, messageId, token, isFireAndForgetMethod)


    /** A method for deserialization request.
      *
      * @param message a structure that contains a binary body of request.
      * @return a request
      */
    @inline
    final def decodeRequest(message: Message): Request = {
      val iprot = protocolReq.getProtocol(new TMemoryInputTransport(message.body))
      codecReq.decode(iprot)
    }


    @inline
    final def decodeRequest(body: Array[Byte]): Request = {
      val iprot = protocolReq.getProtocol(new TMemoryInputTransport(body))
      codecReq.decode(iprot)
    }

    @inline
    final def decodeResponse(body: Array[Byte]): Response = {
      val iprot = protocolRep.getProtocol(new TMemoryInputTransport(body))
      codecRep.decode(iprot)
    }


    /** A method for deserialization response.
      *
      * @param message a structure that contains a binary body of response.
      * @return a response
      */
    @inline
    final def decodeResponse(message: Message): Response = {
      val iprot = protocolRep.getProtocol(new TMemoryInputTransport(message.body))
      codecRep.decode(iprot)
    }

    @inline
    final def responseFromByteArray(bytes: Array[Byte], protocol: TProtocolFactory): Response = {
      val iprot = protocol.getProtocol(new TMemoryInputTransport(bytes))
      codecRep.decode(iprot)
    }

    @inline
    final def requestFromByteArray(bytes: Array[Byte], protocol: TProtocolFactory): Request = {
      val iprot = protocol.getProtocol(new TMemoryInputTransport(bytes))
      codecReq.decode(iprot)
    }
  }

  private val protocolTCompactFactory = new TCompactProtocol.Factory
  private val protocolTBinaryFactory  = new TBinaryProtocol.Factory
  private val protocolJsonFactory     = new TJSONProtocol.Factory

  /** get byte by protocol  */
  def getProtocolID(protocol: TProtocolFactory): Byte = protocol match {
    case `protocolTCompactFactory` => 0
    case `protocolTBinaryFactory`  => 1
    case `protocolJsonFactory`     => 2
  }

  /** get protocol by byte  */
  def getIdProtocol(byte: Byte): TProtocolFactory = byte match {
    case 0 => protocolTCompactFactory
    case 1 => protocolTBinaryFactory
    case 2 => protocolJsonFactory
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

  final def methodWithArgsToString(id: Long, struct: ThriftStruct): String = {
    def toString(methodName: String, arguments: Iterator[Any], fields: List[String]) = {
      val argumentsList = arguments.toList
      fields zip argumentsList mkString(s"request id $id - $methodName: ", " ", "")
    }
    struct match {
      case struct: TransactionService.GetCommitLogOffsets.Args  =>
        toString(GetCommitLogOffsets.name, struct.productIterator, TransactionService.GetCommitLogOffsets.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutStream.Args         =>
        toString(PutStream.name, struct.productIterator, TransactionService.PutStream.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.CheckStreamExists.Args =>
        toString(CheckStreamExists.name, struct.productIterator, TransactionService.CheckStreamExists.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetStream.Args         =>
        toString(GetStream.name, struct.productIterator, TransactionService.GetStream.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.DelStream.Args         =>
        toString(DelStream.name, struct.productIterator, TransactionService.DelStream.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutTransaction.Args    =>
        toString(PutTransaction.name, struct.productIterator, TransactionService.PutTransaction.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutSimpleTransactionAndData.Args =>
        toString(PutSimpleTransactionAndData.name, struct.productIterator, TransactionService.PutSimpleTransactionAndData.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutTransactions.Args   =>
        toString(PutTransactions.name, struct.productIterator, TransactionService.PutTransactions.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetTransaction.Args    =>
        toString(GetTransaction.name, struct.productIterator, TransactionService.GetTransaction.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetLastCheckpointedTransaction.Args =>
        toString(GetLastCheckpointedTransaction.name, struct.productIterator, TransactionService.GetLastCheckpointedTransaction.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.ScanTransactions.Args   =>
        toString(ScanTransactions.name, struct.productIterator, TransactionService.ScanTransactions.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutTransactionData.Args  =>
        toString(PutTransactionData.name, struct.productIterator, TransactionService.PutTransactionData.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetTransactionData.Args  =>
        toString(GetTransactionData.name, struct.productIterator, TransactionService.GetTransactionData.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.PutConsumerCheckpoint.Args =>
        toString(PutConsumerCheckpoint.name, struct.productIterator, TransactionService.PutConsumerCheckpoint.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.GetConsumerState.Args =>
        toString(GetConsumerState.name, struct.productIterator, TransactionService.GetConsumerState.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.Authenticate.Args   =>
        toString(Authenticate.name, struct.productIterator, TransactionService.Authenticate.Args.fieldInfos.map(_.tfield.name))
      case struct: TransactionService.IsValid.Args   =>
        toString(IsValid.name, struct.productIterator, TransactionService.IsValid.Args.fieldInfos.map(_.tfield.name))
      case struct => throw new NotImplementedError(s"$struct is not implemeted for debug information")
    }
  }

  case object GetCommitLogOffsets extends
    Descriptor(getCommitLogOffsetsMethod, 0:Byte, TransactionService.GetCommitLogOffsets.Args, TransactionService.GetCommitLogOffsets.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object PutStream extends
    Descriptor(putStreamMethod, 1:Byte, TransactionService.PutStream.Args, TransactionService.PutStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object CheckStreamExists extends
    Descriptor(checkStreamExists, 2:Byte, TransactionService.CheckStreamExists.Args, TransactionService.CheckStreamExists.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object GetStream extends
    Descriptor(getStreamMethod, 3:Byte, TransactionService.GetStream.Args, TransactionService.GetStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object DelStream extends
    Descriptor(delStreamMethod, 4:Byte, TransactionService.DelStream.Args, TransactionService.DelStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object PutTransaction extends
    Descriptor(putTransactionMethod, 5:Byte, TransactionService.PutTransaction.Args, TransactionService.PutTransaction.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object PutTransactions extends
    Descriptor(putTransactionsMethod, 6:Byte, TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result, protocolTCompactFactory, protocolTBinaryFactory)

  case object PutSimpleTransactionAndData extends
    Descriptor(putSimpleTransactionAndDataMethod, 7:Byte, TransactionService.PutSimpleTransactionAndData.Args, TransactionService.PutSimpleTransactionAndData.Result, protocolTCompactFactory, protocolTBinaryFactory)

  case object GetTransaction extends
    Descriptor(getTransactionMethod, 8:Byte, TransactionService.GetTransaction.Args, TransactionService.GetTransaction.Result, protocolTBinaryFactory, protocolTCompactFactory)

  case object GetLastCheckpointedTransaction extends
    Descriptor(getLastCheckpointedTransactionMethod, 9:Byte, TransactionService.GetLastCheckpointedTransaction.Args, TransactionService.GetLastCheckpointedTransaction.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object ScanTransactions extends
    Descriptor(scanTransactionsMethod, 10:Byte, TransactionService.ScanTransactions.Args, TransactionService.ScanTransactions.Result, protocolTBinaryFactory, protocolTCompactFactory)

  case object PutTransactionData extends
    Descriptor(putTransactionDataMethod, 11:Byte, TransactionService.PutTransactionData.Args, TransactionService.PutTransactionData.Result, protocolTCompactFactory, protocolTBinaryFactory)

  case object GetTransactionData extends
    Descriptor(getTransactionDataMethod, 12:Byte, TransactionService.GetTransactionData.Args, TransactionService.GetTransactionData.Result, protocolTBinaryFactory, protocolTCompactFactory)

  case object PutConsumerCheckpoint extends
    Descriptor(putConsumerCheckpointMethod, 13:Byte, TransactionService.PutConsumerCheckpoint.Args, TransactionService.PutConsumerCheckpoint.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object GetConsumerState extends
    Descriptor(getConsumerStateMethod, 14:Byte, TransactionService.GetConsumerState.Args, TransactionService.GetConsumerState.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object Authenticate extends
    Descriptor(authenticateMethod, 15:Byte, TransactionService.Authenticate.Args, TransactionService.Authenticate.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object IsValid extends
    Descriptor(isValidMethod, 16:Byte, TransactionService.IsValid.Args, TransactionService.IsValid.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  lazy val methods = Array(
    GetCommitLogOffsets,
    PutStream,
    CheckStreamExists,
    GetStream,
    DelStream,
    PutTransaction,
    PutTransactions,
    PutSimpleTransactionAndData,
    GetTransaction,
    GetLastCheckpointedTransaction,
    ScanTransactions,
    PutTransactionData,
    GetTransactionData,
    PutConsumerCheckpoint,
    GetConsumerState,
    Authenticate,
    IsValid
  )
}
