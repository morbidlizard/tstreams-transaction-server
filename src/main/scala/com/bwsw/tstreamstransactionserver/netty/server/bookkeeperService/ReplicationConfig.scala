package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

case class ReplicationConfig(ensembleNumber: Int,
                             writeQuorumNumber: Int,
                             ackQuorumNumber: Int)
