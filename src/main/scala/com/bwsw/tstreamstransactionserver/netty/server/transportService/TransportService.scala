package com.bwsw.tstreamstransactionserver.netty.server.transportService

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.Message
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions

final class TransportService(packageTransmissionOpts: TransportOptions) {
  lazy val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${packageTransmissionOpts.maxMetadataPackageSize}) " +
    s"or maxDataPackageSize (${packageTransmissionOpts.maxDataPackageSize}).")


  val maxMetadataPackageSize: Int =
    packageTransmissionOpts.maxMetadataPackageSize

  val maxDataPackageSize: Int =
    packageTransmissionOpts.maxDataPackageSize

  def isTooBigMetadataMessage(message: Message): Boolean = {
    message.body.length > maxMetadataPackageSize
  }

  def isTooBigDataMessage(message: Message): Boolean = {
    message.body.length > maxDataPackageSize
  }
}
