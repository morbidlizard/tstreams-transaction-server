package com.bwsw.tstreamstransactionserver.netty.server.handler.test
import com.bwsw.tstreamstransactionserver.netty.Message
import io.netty.channel.ChannelHandlerContext

import scala.collection.Searching._

abstract class HandlersRouter(handlers: IndexedSeq[ClientRequestHandler])
  extends LastRequestHandler {

  private val sortedHandlersById =
    handlers.sorted
  private val handlersIDs = sortedHandlersById
    .map(_.id)
  override final def process(message: Message,
                             ctx: ChannelHandlerContext,
                             acc: Option[Throwable]): Unit = {
    handlersIDs.search(message.methodId) match {
      case Found(index) =>
        sortedHandlersById(index).process(message, ctx, acc)
      case _ =>
        throw new IllegalArgumentException(
          s"Not implemented method that has id: ${message.methodId}"
        )
    }
  }
}
