package transactionService.server.db

import com.sleepycat.je.rep.{StateChangeEvent, StateChangeListener}

trait BerkeleyDBListener extends StateChangeListener {
  @volatile var master: Option[String] = None

  @throws[RuntimeException]
  override def stateChange(stateChangeEvent: StateChangeEvent): Unit = {
    import com.sleepycat.je.rep.ReplicatedEnvironment.State._
    stateChangeEvent.getState match {
      case MASTER => master = Some(stateChangeEvent.getMasterNodeName)
      case _ => master = None
    }
  }
}
