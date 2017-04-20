package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MethodDoesnotFoundException, ServerUnreachableException}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message}
import com.twitter.scrooge.ThriftStruct
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}

@Sharable
class ClientHandler(private val reqIdToRep: ConcurrentHashMap[Long, ScalaPromise[ThriftStruct]], val client: Client,
                    implicit val context: ExecutionContext)
  extends SimpleChannelInboundHandler[Message] {

  private def retryCompletePromise(messageSeqId: Long, response: ThriftStruct): Unit = {
    val request = reqIdToRep.get(messageSeqId)
    if (request != null) request.trySuccess(response)
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    def invokeMethod(message: Message)(implicit context: ExecutionContext): ScalaFuture[Unit] = ScalaFuture {
      val response = message.method match {
        case Descriptors.GetCommitLogOffsets.methodID =>
          Descriptors.GetCommitLogOffsets.decodeResponse(message)

        case Descriptors.PutStream.methodID =>
          Descriptors.PutStream.decodeResponse(message)

        case Descriptors.CheckStreamExists.methodID =>
          Descriptors.CheckStreamExists.decodeResponse(message)

        case Descriptors.GetStream.methodID =>
          Descriptors.GetStream.decodeResponse(message)

        case Descriptors.DelStream.methodID =>
          Descriptors.DelStream.decodeResponse(message)

        case Descriptors.PutTransaction.methodID =>
          Descriptors.PutTransaction.decodeResponse(message)

        case Descriptors.PutTransactions.methodID =>
          Descriptors.PutTransactions.decodeResponse(message)

        case Descriptors.PutSimpleTransactionAndData.methodID =>
          Descriptors.PutSimpleTransactionAndData.decodeResponse(message)

        case Descriptors.GetTransaction.methodID =>
          Descriptors.GetTransaction.decodeResponse(message)

        case Descriptors.GetLastCheckpointedTransaction.methodID =>
          Descriptors.GetLastCheckpointedTransaction.decodeResponse(message)

        case Descriptors.ScanTransactions.methodID =>
          Descriptors.ScanTransactions.decodeResponse(message)

        case Descriptors.PutTransactionData.methodID =>
          Descriptors.PutTransactionData.decodeResponse(message)

        case Descriptors.GetTransactionData.methodID =>
          Descriptors.GetTransactionData.decodeResponse(message)

        case Descriptors.PutConsumerCheckpoint.methodID =>
          Descriptors.PutConsumerCheckpoint.decodeResponse(message)

        case Descriptors.GetConsumerState.methodID =>
          Descriptors.GetConsumerState.decodeResponse(message)

        case Descriptors.Authenticate.methodID =>
          Descriptors.Authenticate.decodeResponse(message)

        case Descriptors.IsValid.methodID =>
          Descriptors.IsValid.decodeResponse(message)

        case methodByte =>
          val throwable = new MethodDoesnotFoundException(methodByte.toString)
          ctx.fireExceptionCaught(throwable)
          throw throwable
      }
      retryCompletePromise(message.id, response)
    }

    invokeMethod(msg)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    import scala.collection.JavaConverters._

    reqIdToRep.asScala.foreach{
      case (key, request) if !request.isCompleted =>
        request.tryFailure(new ServerUnreachableException(ctx.name()))
        ScalaFuture(reqIdToRep.remove(key))
    }

    if (!client.isShutdown) {
      ctx.channel().eventLoop().execute(() => client.reconnect())
    }
    super.channelInactive(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}