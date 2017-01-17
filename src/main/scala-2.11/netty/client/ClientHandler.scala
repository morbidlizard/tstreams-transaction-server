package netty.client

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.twitter.scrooge.ThriftStruct
import exception.Throwables.ServerUnreachableException
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import netty.{Descriptors, Message}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}

@Sharable
class ClientHandler(private val reqIdToRep: ConcurrentHashMap[Int, ScalaPromise[ThriftStruct]], val client: Client,
                    implicit val context: ExecutionContext)
  extends SimpleChannelInboundHandler[Message] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    import Descriptors._

    @tailrec
    def retryCompletePromise(messageSeqId: Int, response: ThriftStruct): Unit = {
      if (reqIdToRep.containsKey(messageSeqId))
        reqIdToRep.get(messageSeqId).success(response)
      else
        retryCompletePromise(messageSeqId, response)
    }

    def invokeMethod(message: Message)(implicit context: ExecutionContext): ScalaFuture[Unit] = ScalaFuture {
      val (method, messageSeqId) = Descriptor.decodeMethodName(message)
      val response = method match {
        case `putStreamMethod` =>
          Descriptors.PutStream.decodeResponse(message)

        case `doesStreamExistMethod` =>
          Descriptors.DoesStreamExist.decodeResponse(message)

        case `getStreamMethod` =>
          Descriptors.GetStream.decodeResponse(message)

        case `delStreamMethod` =>
          Descriptors.DelStream.decodeResponse(message)

        case `putTransactionMethod` =>
          Descriptors.PutTransaction.decodeResponse(message)

        case `putTranscationsMethod` =>
          Descriptors.PutTransactions.decodeResponse(message)

        case `scanTransactionsMethod` =>
          Descriptors.ScanTransactions.decodeResponse(message)

        case `putTransactionDataMethod` =>
          Descriptors.PutTransactionData.decodeResponse(message)

        case `getTransactionDataMethod` =>
          Descriptors.GetTransactionData.decodeResponse(message)

        case `setConsumerStateMethod` =>
          Descriptors.SetConsumerState.decodeResponse(message)

        case `getConsumerStateMethod` =>
          Descriptors.GetConsumerState.decodeResponse(message)

        case `authenticateMethod` =>
          Descriptors.Authenticate.decodeResponse(message)

        case `isValidMethod` =>
          Descriptors.IsValid.decodeResponse(message)
      }
      retryCompletePromise(messageSeqId, response)
    }

    invokeMethod(msg)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    import scala.collection.JavaConversions._
    println(reqIdToRep.values())
    for (promise <- reqIdToRep.values()) {
      if (!promise.isCompleted) promise.tryFailure(new ServerUnreachableException)
    }

    ctx.channel().eventLoop().execute(new Runnable() {override def run() {client.connect()}})

    super.channelInactive(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}