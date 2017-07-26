package com.bwsw.tstreamstransactionserver.netty.server.transportService

import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions

final class TransportService(packageTransmissionOpts: TransportOptions) {
  def isTooBigMetadataMessage(bytes: Array[Byte]): Boolean = {
    bytes.length > packageTransmissionOpts.maxMetadataPackageSize
  }

  def isTooBigDataMessage(bytes: Array[Byte]): Boolean = {
    bytes.length > packageTransmissionOpts.maxDataPackageSize
  }
}
