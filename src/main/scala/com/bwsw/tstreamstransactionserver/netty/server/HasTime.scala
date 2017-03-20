package com.bwsw.tstreamstransactionserver.netty.server

trait HasTime {
  def getCurrentTime: Long = System.currentTimeMillis()
}
