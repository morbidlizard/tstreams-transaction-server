/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MethodDoesNotFoundException, ServerUnreachableException}
import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}
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
        case Protocol.GetCommitLogOffsets.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Protocol.GetCommitLogOffsets.decodeResponse(message)))

        case Protocol.PutStream.methodID =>
          retryCompletePromise(message.id, Protocol.PutStream.decodeResponse(message))

        case Protocol.CheckStreamExists.methodID =>
          retryCompletePromise(message.id, Protocol.CheckStreamExists.decodeResponse(message))

        case Protocol.GetStream.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Protocol.GetStream.decodeResponse(message)))(context)

        case Protocol.DelStream.methodID =>
          retryCompletePromise(message.id, Protocol.DelStream.decodeResponse(message))

        case Protocol.GetTransactionID.methodID =>
          retryCompletePromise(message.id, Protocol.GetTransactionID.decodeResponse(message))

        case Protocol.GetTransactionIDByTimestamp.methodID =>
          retryCompletePromise(message.id, Protocol.GetTransactionIDByTimestamp.decodeResponse(message))

        case Protocol.PutTransaction.methodID =>
          retryCompletePromise(message.id, Protocol.PutTransaction.decodeResponse(message))

        case Protocol.PutTransactions.methodID =>
          retryCompletePromise(message.id, Protocol.PutTransactions.decodeResponse(message))

        case Protocol.PutProducerStateWithData.methodID =>
          retryCompletePromise(message.id, Protocol.PutProducerStateWithData.decodeResponse(message))

        case Protocol.PutSimpleTransactionAndData.methodID =>
          retryCompletePromise(message.id, Protocol.PutSimpleTransactionAndData.decodeResponse(message))

        case Protocol.OpenTransaction.methodID =>
          retryCompletePromise(message.id, Protocol.OpenTransaction.decodeResponse(message))

        case Protocol.GetTransaction.methodID =>
          retryCompletePromise(message.id, Protocol.GetTransaction.decodeResponse(message))

        case Protocol.GetLastCheckpointedTransaction.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Protocol.GetLastCheckpointedTransaction.decodeResponse(message)))

        case Protocol.ScanTransactions.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Protocol.ScanTransactions.decodeResponse(message)))(context)

        case Protocol.PutTransactionData.methodID =>
          retryCompletePromise(message.id, Protocol.PutTransactionData.decodeResponse(message))

        case Protocol.GetTransactionData.methodID =>
          ScalaFuture(retryCompletePromise(message.id, Protocol.GetTransactionData.decodeResponse(message)))(context)

        case Protocol.PutConsumerCheckpoint.methodID =>
          retryCompletePromise(message.id, Protocol.PutConsumerCheckpoint.decodeResponse(message))

        case Protocol.GetConsumerState.methodID =>
          retryCompletePromise(message.id, Protocol.GetConsumerState.decodeResponse(message))

        case Protocol.Authenticate.methodID =>
          retryCompletePromise(message.id, Protocol.Authenticate.decodeResponse(message))

        case Protocol.IsValid.methodID =>
          retryCompletePromise(message.id, Protocol.IsValid.decodeResponse(message))

        case methodByte =>
          val throwable = new MethodDoesNotFoundException(methodByte.toString)
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