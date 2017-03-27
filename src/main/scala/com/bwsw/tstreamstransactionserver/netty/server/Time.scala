package com.bwsw.tstreamstransactionserver.netty.server

trait Time {
  def getCurrentTime: Long = System.currentTimeMillis()
}
