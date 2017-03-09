package com.bwsw.commitlog

/** Policies to work with commitlog. */
object CommitLogFlushPolicy {
  /** Basic trait for all policies */
  trait ICommitLogFlushPolicy

  /** Data is flushed into file when new file starts. */
  case object OnRotation extends ICommitLogFlushPolicy

  /** Data is flushed into file when specified count of seconds from last flush operation passed. */
  case class OnTimeInterval(seconds: Integer) extends ICommitLogFlushPolicy {
    require(seconds > 0, "Interval of seconds must be greater that 0.")
  }

  /** Data is flushed into file when specified count of write operations passed. */
  case class OnCountInterval(count: Integer) extends ICommitLogFlushPolicy {
    require(count > 0, "Interval of writes must be greater that 0.")
  }
}
