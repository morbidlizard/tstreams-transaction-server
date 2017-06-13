package com.bwsw.tstreamstransactionserver.netty.server.commitLogService.bookkeeper

trait ServerRole {
  def hasLeadership: Boolean
}
