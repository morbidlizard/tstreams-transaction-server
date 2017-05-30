package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.Message

trait MessageToTransactionServer {
  def processRequest(message: Message): Message
}
