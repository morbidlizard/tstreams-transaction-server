package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy

trait EntityPathConverter[T]
{
  def entityToPath(entity: T): Array[String]
}
