package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MethodDoesnotFoundException, ServerUnreachableException}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, Message}
import com.twitter.scrooge.ThriftStruct
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}

@Sharable
class ClientHandler(private val reqIdToRep: ConcurrentHashMap[Long, ScalaPromise[ThriftStruct]], val client: Client,
                    implicit val context: ExecutionContext)
  extends SimpleChannelInboundHandler[ByteBuf] {

  private def retryCompletePromise(messageSeqId: Long, response: ThriftStruct): Unit = {
    val request = reqIdToRep.get(messageSeqId)
    if (request != null) request.trySuccess(response)
  }

  override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
    def invokeMethod(message: Message)(implicit context: ExecutionContext):Unit =  {
      message.method match {
        case Descriptors.GetCommitLogOffsets.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Descriptors.GetCommitLogOffsets.decodeResponse(message)))

        case Descriptors.PutStream.methodID =>
          retryCompletePromise(message.id, Descriptors.PutStream.decodeResponse(message)) 

        case Descriptors.CheckStreamExists.methodID =>
          retryCompletePromise(message.id, Descriptors.CheckStreamExists.decodeResponse(message)) 

        case Descriptors.GetStream.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Descriptors.GetStream.decodeResponse(message)))(context)

        case Descriptors.DelStream.methodID =>
          retryCompletePromise(message.id, Descriptors.DelStream.decodeResponse(message))

        case Descriptors.GetTransactionID.methodID =>
          retryCompletePromise(message.id, Descriptors.GetTransactionID.decodeResponse(message))

        case Descriptors.GetTransactionIDByTimestamp.methodID =>
          retryCompletePromise(message.id, Descriptors.GetTransactionIDByTimestamp.decodeResponse(message))

        case Descriptors.PutTransaction.methodID =>
          retryCompletePromise(message.id, Descriptors.PutTransaction.decodeResponse(message)) 

        case Descriptors.PutTransactions.methodID =>
          retryCompletePromise(message.id, Descriptors.PutTransactions.decodeResponse(message))

        case Descriptors.PutProducerStateWithData.methodID =>
          retryCompletePromise(message.id, Descriptors.PutProducerStateWithData.decodeResponse(message))

        case Descriptors.PutSimpleTransactionAndData.methodID =>
          retryCompletePromise(message.id, Descriptors.PutSimpleTransactionAndData.decodeResponse(message))

        case Descriptors.OpenTransaction.methodID =>
          retryCompletePromise(message.id, Descriptors.OpenTransaction.decodeResponse(message))

        case Descriptors.GetTransaction.methodID =>
          retryCompletePromise(message.id, Descriptors.GetTransaction.decodeResponse(message)) 

        case Descriptors.GetLastCheckpointedTransaction.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Descriptors.GetLastCheckpointedTransaction.decodeResponse(message)))

        case Descriptors.ScanTransactions.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Descriptors.ScanTransactions.decodeResponse(message)))(context)

        case Descriptors.PutTransactionData.methodID =>
          retryCompletePromise(message.id, Descriptors.PutTransactionData.decodeResponse(message)) 

        case Descriptors.GetTransactionData.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Descriptors.GetTransactionData.decodeResponse(message)))(context)

        case Descriptors.PutConsumerCheckpoint.methodID =>
          retryCompletePromise(message.id, Descriptors.PutConsumerCheckpoint.decodeResponse(message)) 

        case Descriptors.GetConsumerState.methodID =>
          retryCompletePromise(message.id, Descriptors.GetConsumerState.decodeResponse(message)) 

        case Descriptors.Authenticate.methodID =>
          retryCompletePromise(message.id, Descriptors.Authenticate.decodeResponse(message)) 

        case Descriptors.IsValid.methodID =>
          retryCompletePromise(message.id, Descriptors.IsValid.decodeResponse(message)) 

        case methodByte =>
          val throwable = new MethodDoesnotFoundException(methodByte.toString)
          ctx.fireExceptionCaught(throwable)
          throw throwable
      }
    }
    val message = Message.fromByteBuf(buf)
    invokeMethod(message)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    ctx.fireChannelInactive()

    val remoteAddress = ctx.channel().remoteAddress().toString
    reqIdToRep.forEach((t: Long, promise: ScalaPromise[ThriftStruct]) => {
      promise.tryFailure(new ServerUnreachableException(remoteAddress))
    })

    if (!client.isShutdown) {
      ScalaFuture(ctx.channel().eventLoop().execute(() => client.reconnect()))
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}