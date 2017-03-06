package com.bwsw.tstreamstransactionserver.options

object CommitLogWriteSyncPolicy extends Enumeration {
  type CommitLogWriteSyncPolicy = Value
  val EveryNSeconds = Value("every-n-seconds")
  val EveryNth = Value("every-nth")
  val EveryNewFile = Value("every-new-file")
}