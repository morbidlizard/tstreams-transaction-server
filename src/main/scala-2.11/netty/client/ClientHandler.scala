package netty.client

import java.util.concurrent.ConcurrentHashMap


import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import netty.{Descriptors, Message}

import scala.concurrent.{Promise => ScalaPromise}

class ClientHandler(reqIdToRep: ConcurrentHashMap[Int, ScalaPromise[FunctionResult.Result]]) extends SimpleChannelInboundHandler[Message] {


  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    import Descriptors._
    def invokeMethod(message: Message): Unit = {
      val (method, messageSeqId) = Descriptor.decodeMethodName(message)
      method match {
        case `putStreamMethod` =>
          val response = Descriptors.PutStream.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.BoolResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `doesStreamExistMethod` =>
          val response = Descriptors.DoesStreamExist.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.BoolResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `getStreamMethod` =>
          val response = Descriptors.GetStream.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.StreamResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `delStreamMethod` =>
          val response = Descriptors.DelStream.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.BoolResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `putTransactionMethod` =>
          val response = Descriptors.PutTransaction.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.BoolResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `putTranscationsMethod` =>
          val response = Descriptors.PutTransactions.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.BoolResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `scanTransactionsMethod` =>
          val response = Descriptors.ScanTransactions.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.SeqTransactionsResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `putTransactionDataMethod` =>
          val response = Descriptors.PutTransactionData.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.BoolResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))


        case `getTransactionDataMethod` =>
          val response = Descriptors.GetTransactionData.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.SeqByteBufferResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `setConsumerStateMethod` =>
          val response = Descriptors.SetConsumerState.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.BoolResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `getConsumerStateMethod` =>
          val response = Descriptors.GetConsumerState.decodeResponse(message)
          if (response.success.isDefined)
            reqIdToRep.get(messageSeqId).success(new FunctionResult.LongResult(response.success.get))
          else
            reqIdToRep.get(messageSeqId).failure(new FunctionResult.TokenInvalidExceptionResult(response.tokenInvalid.get))

        case `authenticateMethod` =>
          reqIdToRep.get(messageSeqId).success(new FunctionResult.IntResult(Descriptors.Authenticate.decodeResponse(message).success.get))

        case `isValidMethod` =>
          reqIdToRep.get(messageSeqId).success(new FunctionResult.BoolResult(Descriptors.IsValid.decodeResponse(message).success.get))
      }
    }
    invokeMethod(msg)
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}