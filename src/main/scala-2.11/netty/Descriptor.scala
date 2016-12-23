package netty

object Descriptor extends Enumeration {
  type Descriptor = Value
  val
  PutStream, DoesStreamExist, GetStream, DelStream,

  PutTransaction, PutTransactions, ScanTransactions,

  PutTransactionData, GetTransactionData,

  SetConsumerState, GetConsumerState,

  Authenticate, IsValid = Value

  def apply(byte: Byte): Descriptor = Descriptor(byte.toInt)
}