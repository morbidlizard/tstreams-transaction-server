package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

trait Electable {
  def hasLeadership: Boolean
  def stopParticipateInElection(): Unit
}
