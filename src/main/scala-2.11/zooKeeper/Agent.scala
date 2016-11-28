package zooKeeper

case class Agent(address: String, port: Int, id: Int)
{
  require(port > 0)
  def name = toString
  override def toString: String = s"$address:$port"
}
