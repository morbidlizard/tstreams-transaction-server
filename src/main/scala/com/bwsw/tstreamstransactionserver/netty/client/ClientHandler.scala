package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MethodDoesnotFoundException, ServerUnreachableException}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message}
import com.google.common.cache.Cache
import com.twitter.scrooge.ThriftStruct
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}

@Sharable
class ClientHandler(private val reqIdToRep: Cache[Integer, ScalaPromise[ThriftStruct]], val client: Client,
                    implicit val context: ExecutionContext)
  extends SimpleChannelInboundHandler[Message] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    import Descriptors._

    def retryCompletePromise(messageSeqId: Int, response: ThriftStruct): Unit = {
      val request = reqIdToRep.getIfPresent(messageSeqId)
      if (request != null) request.trySuccess(response)
    }

    def invokeMethod(message: Message)(implicit context: ExecutionContext): ScalaFuture[Unit] = ScalaFuture {
      val (method, messageSeqId) = Descriptor.decodeMethodName(message)
      val response = method match {
        case `putStreamMethod` =>
          Descriptors.PutStream.decodeResponse(message)

        case `checkStreamExists` =>
          Descriptors.CheckStreamExists.decodeResponse(message)

        case `getStreamMethod` =>
          Descriptors.GetStream.decodeResponse(message)

        case `delStreamMethod` =>
          Descriptors.DelStream.decodeResponse(message)

        case `putTransactionMethod` =>
          Descriptors.PutTransaction.decodeResponse(message)

        case `putTranscationsMethod` =>
          Descriptors.PutTransactions.decodeResponse(message)

        case `getTransactionMethod` =>
          Descriptors.GetTransaction.decodeResponse(message)

        case `getLastCheckpointedTransactionMethod` =>
          Descriptors.GetLastCheckpointedTransaction.decodeResponse(message)

        case `scanTransactionsMethod` =>
          Descriptors.ScanTransactions.decodeResponse(message)

        case `putTransactionDataMethod` =>
          Descriptors.PutTransactionData.decodeResponse(message)

        case `getTransactionDataMethod` =>
          Descriptors.GetTransactionData.decodeResponse(message)

        case `putConsumerCheckpointMethod` =>
          Descriptors.PutConsumerCheckpoint.decodeResponse(message)

        case `getConsumerStateMethod` =>
          Descriptors.GetConsumerState.decodeResponse(message)

        case `authenticateMethod` =>
          Descriptors.Authenticate.decodeResponse(message)

        case `isValidMethod` =>
          Descriptors.IsValid.decodeResponse(message)

        case _ =>
          val throwable = new MethodDoesnotFoundException(method)
          ctx.fireExceptionCaught(throwable)
          throw throwable
      }
      retryCompletePromise(messageSeqId, response)
    }



    invokeMethod(msg)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {

    reqIdToRep.asMap().values()
      .forEach(request => if (!request.isCompleted) request.tryFailure(new ServerUnreachableException(ctx.name())))

    ctx.channel().eventLoop().execute(() => client.reconnect())

    super.channelInactive(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}