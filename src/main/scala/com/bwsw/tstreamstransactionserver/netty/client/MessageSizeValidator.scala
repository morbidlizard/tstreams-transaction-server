package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.{Message, Protocol}


private object MessageSizeValidator {

  val notValidateMessageProtocolIds: Array[Byte] =
    Array(
      Protocol.GetMaxPackagesSizes.methodID,
      Protocol.Authenticate.methodID,
      Protocol.IsValid.methodID
    )

  val metadataMessageProtocolIds: Array[Byte] =
    Array(
      Protocol.GetCommitLogOffsets.methodID,
      Protocol.GetLastCheckpointedTransaction.methodID,
      Protocol.GetTransaction.methodID,
      Protocol.GetTransactionID.methodID,
      Protocol.GetTransactionIDByTimestamp.methodID,
      Protocol.OpenTransaction.methodID,
      Protocol.PutTransaction.methodID,
      Protocol.PutTransactions.methodID,
      Protocol.ScanTransactions.methodID,

      Protocol.PutConsumerCheckpoint.methodID,
      Protocol.GetConsumerState.methodID
    )

  val dataMessageProtocolIds: Array[Byte] =
    Array(
      Protocol.GetTransactionData.methodID,
      Protocol.PutProducerStateWithData.methodID,
      Protocol.PutSimpleTransactionAndData.methodID,
      Protocol.PutTransactionData.methodID
    )
}

final class MessageSizeValidator(maxMetadataPackageSize: Int,
                                 maxDataPackageSize: Int) {

  private def notValidateSomeMessageTypesSize(message: Message) = {
    if (MessageSizeValidator.notValidateMessageProtocolIds.contains(message.method)) {
      //do nothing
    }
    else {
      validateMetadataMessageSize(message)
    }
  }

  @throws[PackageTooBigException]
  private def validateMetadataMessageSize(message: Message) = {
    if (MessageSizeValidator.metadataMessageProtocolIds.contains(message.method)) {
      if (message.length > maxMetadataPackageSize)  {
        throw new PackageTooBigException(s"Client shouldn't transmit amount of data which is greater " +
          s"than maxMetadataPackageSize ($maxMetadataPackageSize).")
      }
    }
    else {
      validateDataMessageSize(message)
    }

  }

  @throws[PackageTooBigException]
  private def validateDataMessageSize(message: Message) = {
    if (MessageSizeValidator.dataMessageProtocolIds.contains(message.method)) {
      if (message.length > maxDataPackageSize) {
        throw new PackageTooBigException(s"Client shouldn't transmit amount of data which is greater " +
          s"than maxDataPackageSize ($maxDataPackageSize).")
      }
    }
    else {
      //do nothing
    }
  }


  def validateMessageSize(message: Message): Unit = {
    notValidateSomeMessageTypesSize(message)
  }
}
