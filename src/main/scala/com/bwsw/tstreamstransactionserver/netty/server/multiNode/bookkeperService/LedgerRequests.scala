package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService


import scala.concurrent.Promise

case class LedgerRequests(ledger: org.apache.bookkeeper.client.LedgerHandle,
                          requests: java.util.concurrent.ConcurrentHashMap.KeySetView[Promise[_], java.lang.Boolean] =
                            java.util.concurrent.ConcurrentHashMap.newKeySet[Promise[_]]())
