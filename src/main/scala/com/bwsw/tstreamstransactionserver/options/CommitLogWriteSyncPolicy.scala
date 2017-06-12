package com.bwsw.tstreamstransactionserver.options

object CommitLogWriteSyncPolicy extends Enumeration {
  type CommitLogWriteSyncPolicy = Value
  val EveryNSeconds = Value("every-n-seconds")
  val EveryNth = Value("every-nth")
  val EveryNewFile = Value("every-new-file")
}

object IncompleteCommitLogReadPolicy extends Enumeration {
  type IncompleteCommitLogReadPolicy = Value
  val SkipLog = Value("skip-log")
  val TryRead = Value("try-read")
  val Error = Value("error")
}