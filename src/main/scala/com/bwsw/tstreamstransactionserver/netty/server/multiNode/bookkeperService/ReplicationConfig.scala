package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

case class ReplicationConfig(ensembleNumber: Int,
                             writeQuorumNumber: Int,
                             ackQuorumNumber: Int)
