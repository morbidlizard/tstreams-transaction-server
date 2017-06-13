package com.bwsw.tstreamstransactionserver.netty.server.commitLogService.bookkeeper

import org.apache.zookeeper.data.Stat

case class LedgersWithMetadataInformation(ledgers: Array[Long],
                                          zNodeMetadata: Stat,
                                          mustCreate: Boolean
                                         )
