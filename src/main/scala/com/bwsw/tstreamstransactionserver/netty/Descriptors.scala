package com.bwsw.tstreamstransactionserver.netty

import java.util

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}
import org.apache.thrift.protocol._
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}
import transactionService.rpc.TransactionService

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
  sealed abstract class Descriptor[T <: ThriftStruct, R <: ThriftStruct](methodName: String,
                                                                         codecReq: ThriftStructCodec3[T],
                                                                         codecRep: ThriftStructCodec3[R],
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
    private def encode(entity: ThriftStruct, protocol: TProtocolFactory, messageId: Int, token: Int): Message = {
      val buffer = new TMemoryBuffer(512)
      val oprot = protocol.getProtocol(buffer)

      oprot.writeMessageBegin(new TMessage(methodName, TMessageType.CALL, messageId))
      entity.write(oprot)
      oprot.writeMessageEnd()
      val bytes = util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
      Message(bytes.length, getProtocolID(protocol), bytes, token)
    }

    /** A method for serializing request and adding an id to id. */
    def encodeRequest(entity: T)(messageId: Int, token: Int): Message = encode(entity, protocolReq, messageId, token)

    /** A method for serializing response and adding an id to id. */
    def encodeResponse(entity: R)(messageId: Int, token: Int): Message = encode(entity, protocolRep, messageId, token)


    /** A method for deserialization request.
      *
      *  @param message a structure that contains a binary body of request.
      *  @return a request
      */
    def decodeRequest(message: Message): T = {
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
    def decodeResponse(message: Message): R = {
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
    Descriptor(putStreamMethod, TransactionService.PutStream.Args, TransactionService.PutStream.Result, protocolTBinaryFactory, protocolTBinaryFactory)

  case object CheckStreamExists extends
    Descriptor(doesStreamExistMethod, TransactionService.CheckStreamExists.Args, TransactionService.CheckStreamExists.Result, protocolTBinaryFactory, protocolTBinaryFactory)

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