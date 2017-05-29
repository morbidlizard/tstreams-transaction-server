package com.bwsw.tstreamstransactionserver.netty.server.handler

trait RequestHandler
{
  def handle(requestBody: Array[Byte]): Array[Byte]
}