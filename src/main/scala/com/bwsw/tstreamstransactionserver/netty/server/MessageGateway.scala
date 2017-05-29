package com.bwsw.tstreamstransactionserver.netty.server
import com.bwsw.tstreamstransactionserver.netty.Message

class MessageGateway
  extends MessageToTransactionServer {
  //In order to decouple server handler. 
  override def processRequest(message: Message): Message = ???
}
