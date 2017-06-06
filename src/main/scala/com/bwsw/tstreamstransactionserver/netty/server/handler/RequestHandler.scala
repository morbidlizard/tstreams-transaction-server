package com.bwsw.tstreamstransactionserver.netty.server.handler

trait RequestHandler
{
  def getName: String
  def handleAndGetResponse(requestBody: Array[Byte]): Array[Byte]
  def handle(requestBody: Array[Byte]): Unit
  def createErrorResponse(message: String): Array[Byte]
}