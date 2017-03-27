package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

case class LastOpenedAndCheckpointedTransaction(opened: TransactionID, checkpointed: Option[TransactionID])