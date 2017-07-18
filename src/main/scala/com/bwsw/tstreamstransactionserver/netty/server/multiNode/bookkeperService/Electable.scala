package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

trait Electable {
  def hasLeadership: Boolean
  def stopParticipateInElection(): Unit
}
