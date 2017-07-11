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
class ClientHandler(reqIdToRep: ConcurrentHashMap[Long, ScalaPromise[ThriftStruct]],
                    implicit val context: ExecutionContext)
  extends SimpleChannelInboundHandler[ByteBuf] {

  private def tryCompleteResponse(messageSeqId: Long, response: ThriftStruct): Unit = {
    val request = reqIdToRep.get(messageSeqId)
    if (request != null) request.trySuccess(response)
  }

  override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
    def invokeMethod(message: Message)(implicit context: ExecutionContext):Unit =  {
      message.method match {
        case Protocol.GetCommitLogOffsets.methodID =>
          ScalaFuture(tryCompleteResponse(message.id, Protocol.GetCommitLogOffsets.decodeResponse(message)))

        case Protocol.PutStream.methodID =>
          tryCompleteResponse(message.id, Protocol.PutStream.decodeResponse(message))

        case Protocol.CheckStreamExists.methodID =>
          tryCompleteResponse(message.id, Protocol.CheckStreamExists.decodeResponse(message))

        case Protocol.GetStream.methodID =>
          ScalaFuture(tryCompleteResponse(message.id, Protocol.GetStream.decodeResponse(message)))(context)

        case Protocol.DelStream.methodID =>
          tryCompleteResponse(message.id, Protocol.DelStream.decodeResponse(message))

        case Protocol.GetTransactionID.methodID =>
          tryCompleteResponse(message.id, Protocol.GetTransactionID.decodeResponse(message))

        case Protocol.GetTransactionIDByTimestamp.methodID =>
          tryCompleteResponse(message.id, Protocol.GetTransactionIDByTimestamp.decodeResponse(message))

        case Protocol.PutTransaction.methodID =>
          tryCompleteResponse(message.id, Protocol.PutTransaction.decodeResponse(message))

        case Protocol.PutTransactions.methodID =>
          tryCompleteResponse(message.id, Protocol.PutTransactions.decodeResponse(message))

        case Protocol.PutProducerStateWithData.methodID =>
          tryCompleteResponse(message.id, Protocol.PutProducerStateWithData.decodeResponse(message))

        case Protocol.PutSimpleTransactionAndData.methodID =>
          tryCompleteResponse(message.id, Protocol.PutSimpleTransactionAndData.decodeResponse(message))

        case Protocol.OpenTransaction.methodID =>
          tryCompleteResponse(message.id, Protocol.OpenTransaction.decodeResponse(message))

        case Protocol.GetTransaction.methodID =>
          tryCompleteResponse(message.id, Protocol.GetTransaction.decodeResponse(message))

        case Protocol.GetLastCheckpointedTransaction.methodID =>
          ScalaFuture(tryCompleteResponse(message.id, Protocol.GetLastCheckpointedTransaction.decodeResponse(message)))

        case Protocol.ScanTransactions.methodID =>
          ScalaFuture(tryCompleteResponse(message.id, Protocol.ScanTransactions.decodeResponse(message)))(context)

        case Protocol.PutTransactionData.methodID =>
          tryCompleteResponse(message.id, Protocol.PutTransactionData.decodeResponse(message))

        case Protocol.GetTransactionData.methodID =>
          ScalaFuture(tryCompleteResponse(message.id, Protocol.GetTransactionData.decodeResponse(message)))(context)

        case Protocol.PutConsumerCheckpoint.methodID =>
          tryCompleteResponse(message.id, Protocol.PutConsumerCheckpoint.decodeResponse(message))

        case Protocol.GetConsumerState.methodID =>
          tryCompleteResponse(message.id, Protocol.GetConsumerState.decodeResponse(message))

        case Protocol.Authenticate.methodID =>
          tryCompleteResponse(message.id, Protocol.Authenticate.decodeResponse(message))

        case Protocol.IsValid.methodID =>
          tryCompleteResponse(message.id, Protocol.IsValid.decodeResponse(message))

        case Protocol.GetMaxPackagesSizes.methodID =>
          tryCompleteResponse(message.id, Protocol.GetMaxPackagesSizes.decodeResponse(message))

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
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}