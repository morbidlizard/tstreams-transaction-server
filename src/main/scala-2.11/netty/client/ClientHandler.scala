package netty.client

import java.util.concurrent.ConcurrentHashMap


import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import netty.{Descriptors, Message}

import scala.concurrent.{Promise => ScalaPromise}
import FunctionResult._

class ClientHandler(reqIdToRep: ConcurrentHashMap[Int, ScalaPromise[FunctionResult.Result]]) extends SimpleChannelInboundHandler[Message] {


  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    import Descriptors._
    def invokeMethod(message: Message): Unit = {
      val (method, messageSeqId) = Descriptor.decodeMethodName(message)
      val result = method match {
        case `putStreamMethod` =>          new FunctionResult.BoolResult(Descriptors.PutStream.decodeResponse(message).success.get)
        case `doesStreamExistMethod` =>    new FunctionResult.BoolResult(Descriptors.DoesStreamExist.decodeResponse(message).success.get)
        case `getStreamMethod` =>          new FunctionResult.StreamResult(Descriptors.GetStream.decodeResponse(message).success.get)
        case `delStreamMethod` =>          new FunctionResult.BoolResult(Descriptors.DelStream.decodeResponse(message).success.get)
        case `putTransactionMethod` =>     new FunctionResult.BoolResult(Descriptors.PutTransaction.decodeResponse(message).success.get)
        case `putTranscationsMethod` =>    new FunctionResult.BoolResult(Descriptors.PutTransactions.decodeResponse(message).success.get)
        case `scanTransactionsMethod` =>   new FunctionResult.SeqTransactionsResult(Descriptors.ScanTransactions.decodeResponse(message).success.get)
        case `putTransactionDataMethod` => new FunctionResult.BoolResult(Descriptors.PutTransactionData.decodeResponse(message).success.get)
        case `getTransactionDataMethod` => new FunctionResult.SeqByteBufferResult(Descriptors.GetTransactionData.decodeResponse(message).success.get)
        case `setConsumerStateMethod` =>   new FunctionResult.BoolResult(Descriptors.SetConsumerState.decodeResponse(message).success.get)
        case `getConsumerStateMethod` =>   new FunctionResult.LongResult(Descriptors.GetConsumerState.decodeResponse(message).success.get)
        case `authenticateMethod` =>       new FunctionResult.IntResult(Descriptors.Authenticate.decodeResponse(message).success.get)
        case `isValidMethod` =>            new FunctionResult.BoolResult(Descriptors.IsValid.decodeResponse(message).success.get)
      }
      reqIdToRep.get(messageSeqId).success(result)
    }
    invokeMethod(msg)
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}