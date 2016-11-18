package zooKeeper

case class Agent(address: String,port: String,id: String, priority: Agent.Priority.Value)
{
  def name = toString
  override def toString: String = s"$address:$port(version:$id)"
}

object Agent {
  object Priority extends Enumeration {
    val Normal, Low = Value
  }
}

