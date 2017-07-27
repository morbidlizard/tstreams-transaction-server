package com.bwsw.tstreamstransactionserver.netty.server.transportService

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions

final class TransportService(packageTransmissionOpts: TransportOptions) {
  lazy val packageTooBigException =
    new PackageTooBigException(
      s"A size of client request is greater " +
        s"than maxMetadataPackageSize (${packageTransmissionOpts.maxMetadataPackageSize}) " +
        s"or maxDataPackageSize (${packageTransmissionOpts.maxDataPackageSize}).")

  def isTooBigMetadataMessage(bytes: Array[Byte]): Boolean = {
    bytes.length > packageTransmissionOpts.maxMetadataPackageSize
  }

  def isTooBigDataMessage(bytes: Array[Byte]): Boolean = {
    bytes.length > packageTransmissionOpts.maxDataPackageSize
  }
}
