package com.bwsw.tstreamstransactionserver.netty.server.handler

import com.bwsw.tstreamstransactionserver.exception
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportService
import io.netty.channel.ChannelHandlerContext

abstract class RequestWithValidationProcessor(authService: AuthService,
                                              transportService: TransportService)
  extends RequestProcessor
{

  def validate(message: Message,
               ctx: ChannelHandlerContext): Option[Exception] = {
    if (!authService.isValid(message.token)) {
      Some(new exception.Throwable.TokenInvalidException)
    }
    else if (transportService.isTooBigDataMessage(message)) {
      Some(transportService.packageTooBigException)
    }
    else {
      None
    }
  }
}
