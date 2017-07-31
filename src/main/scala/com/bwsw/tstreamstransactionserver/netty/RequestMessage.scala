
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

package com.bwsw.tstreamstransactionserver.netty

import io.netty.buffer.ByteBuf


/** Message is a placeholder for some binary information.
  *
  *  @constructor create a new message with body, the size of body, and a protocol to serialize/deserialize the body.
  *  @param bodyLength a size of body.
  *  @param thriftProtocol a protocol to serialize/deserialize the body.
  *  @param body a binary representation of information.
  *
  */
case class RequestMessage(id: Long,
                          bodyLength: Int,
                          thriftProtocol: Byte,
                          body: Array[Byte],
                          token: Int,
                          methodId: Byte,
                          isFireAndForgetMethod: Boolean)
{
  /** Serializes a message. */
  def toByteArray: Array[Byte] = {
    val size = {
      RequestMessage.headerFieldSize +
        RequestMessage.lengthFieldSize +
        body.length
    }

    val isFireAndForgetMethodToByte = {
      if (isFireAndForgetMethod)
        1: Byte
      else
        0: Byte
    }

    val buffer = java.nio.ByteBuffer
      .allocate(size)
      .putLong(id)                //0-8
      .put(thriftProtocol)        //8-9
      .putInt(token)              //9-13
      .put(methodId)              //13-14
      .put(isFireAndForgetMethodToByte) //14-15
      .putInt(bodyLength)         //15-19
      .put(body)                  //20-size
    buffer.flip()

    val binaryMessage = new Array[Byte](size)
    buffer.get(binaryMessage)
    binaryMessage
  }
}
object RequestMessage {
  val headerFieldSize: Byte = (
      java.lang.Long.BYTES +     //id
      java.lang.Byte.BYTES +     //protocol
      java.lang.Integer.BYTES +  //token
      java.lang.Byte.BYTES +     //method
      java.lang.Byte.BYTES       //isFireAndForgetMethod
    ).toByte
  val lengthFieldSize =  java.lang.Integer.BYTES //length

  /** Deserializes a binary to message. */
  def fromByteArray(bytes: Array[Byte]): RequestMessage = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id     = buffer.getLong
    val protocol = buffer.get
    val token = buffer.getInt
    val method = buffer.get()
    val isFireAndForgetMethod = {
      if (buffer.get() == (1: Byte))
        true
      else
        false
    }
    val length = buffer.getInt
    val message = {
      val bytes = new Array[Byte](buffer.limit() - headerFieldSize - lengthFieldSize)
      buffer.get(bytes)
      bytes
    }
    RequestMessage(id, length, protocol, message, token, method, isFireAndForgetMethod)
  }

  def fromByteBuf(buf: ByteBuf): RequestMessage = {
    val id       = buf.readLong()
    val protocol = buf.readByte()
    val token    = buf.readInt()
    val method   = buf.readByte()
    val isFireAndForgetMethod = {
      if (buf.readByte() == (1: Byte))
        true
      else
        false
    }
    val length   = buf.readInt()
    val message = {
      val bytes = new Array[Byte](buf.readableBytes())
      buf.slice()
      buf.readBytes(bytes)
      bytes
    }
    RequestMessage(id, length, protocol, message, token, method, isFireAndForgetMethod)
  }

  def getIdFromByteBuf(buf: ByteBuf): Long = {
    buf.getLong(0)
  }

}

