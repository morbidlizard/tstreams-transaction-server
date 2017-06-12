
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
  *  @param length a size of body.
  *  @param protocol a protocol to serialize/deserialize the body.
  *  @param body a binary representation of information.
  *
  */
case class Message(id: Long, length: Int, protocol: Byte, body: Array[Byte], token: Int, method: Byte, isFireAndForgetMethod: Byte)
{
  /** Serializes a message. */
  def toByteArray: Array[Byte] = {
    val size = Message.headerFieldSize + Message.lengthFieldSize + body.length
    val buffer = java.nio.ByteBuffer
      .allocate(size)
      .putLong(id)
      .put(protocol)
      .putInt(token)
      .put(method)
      .put(isFireAndForgetMethod)
      .putInt(length)
      .put(body)
    buffer.flip()

    val binaryMessage = new Array[Byte](size)
    buffer.get(binaryMessage)
    binaryMessage
  }

}
object Message {
  val headerFieldSize: Byte = (
      java.lang.Long.BYTES +     //id
      java.lang.Byte.BYTES +     //protocol
      java.lang.Integer.BYTES +  //token
      java.lang.Byte.BYTES +     //method
      java.lang.Byte.BYTES       //isFireAndForgetMethod
    ).toByte
  val lengthFieldSize =  java.lang.Integer.BYTES //length

  /** Deserializes a binary to message. */
  def fromByteArray(bytes: Array[Byte]): Message = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val id     = buffer.getLong
    val protocol = buffer.get
    val token = buffer.getInt
    val method = buffer.get()
    val isFireAndForgetMethod = buffer.get()
    val length = buffer.getInt
    val message = {
      val bytes = new Array[Byte](buffer.limit() - headerFieldSize - lengthFieldSize)
      buffer.get(bytes)
      bytes
    }
    Message(id, length, protocol, message, token, method, isFireAndForgetMethod)
  }

  def fromByteBuf(buf: ByteBuf): Message = {
    val id       = buf.readLong()
    val protocol = buf.readByte()
    val token    = buf.readInt()
    val method   = buf.readByte()
    val isFireAndForgetMethod = buf.readByte()
    val length   = buf.readInt()
    val message = {
      val bytes = new Array[Byte](buf.readableBytes())
      buf.readBytes(bytes)
      bytes
    }
    Message(id, length, protocol, message, token, method, isFireAndForgetMethod)
  }

}

