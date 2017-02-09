/**
 * Generated by Scrooge
 *   version: 4.14.0
 *   rev: 56cc6a6ed000a14f1a3fef4e3e5e60dab4478499
 *   built at: 20170203-164626
 */
package transactionService.rpc

import com.twitter.scrooge.{
  HasThriftStructCodec3,
  LazyTProtocol,
  TFieldBlob,
  ThriftException,
  ThriftStruct,
  ThriftStructCodec3,
  ThriftStructFieldInfo,
  ThriftStructMetaData,
  ThriftUtil
}
import org.apache.thrift.protocol._
import org.apache.thrift.transport.{TMemoryBuffer, TTransport}
import java.nio.ByteBuffer
import java.util.Arrays
import scala.collection.immutable.{Map => immutable$Map}
import scala.collection.mutable.Builder
import scala.collection.mutable.{
  ArrayBuffer => mutable$ArrayBuffer, Buffer => mutable$Buffer,
  HashMap => mutable$HashMap, HashSet => mutable$HashSet}
import scala.collection.{Map, Set}


object Transaction extends ThriftStructCodec3[Transaction] {
  private val NoPassthroughFields = immutable$Map.empty[Short, TFieldBlob]
  val Struct = new TStruct("Transaction")
  val ProducerTransactionField = new TField("producerTransaction", TType.STRUCT, 1)
  val ProducerTransactionFieldManifest = implicitly[Manifest[transactionService.rpc.ProducerTransaction]]
  val ConsumerTransactionField = new TField("consumerTransaction", TType.STRUCT, 2)
  val ConsumerTransactionFieldManifest = implicitly[Manifest[transactionService.rpc.ConsumerTransaction]]

  /**
   * Field information in declaration order.
   */
  lazy val fieldInfos: scala.List[ThriftStructFieldInfo] = scala.List[ThriftStructFieldInfo](
    new ThriftStructFieldInfo(
      ProducerTransactionField,
      true,
      false,
      ProducerTransactionFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      ConsumerTransactionField,
      true,
      false,
      ConsumerTransactionFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    )
  )

  lazy val structAnnotations: immutable$Map[String, String] =
    immutable$Map.empty[String, String]

  /**
   * Checks that all required fields are non-null.
   */
  def validate(_item: Transaction): Unit = {
  }

  def withoutPassthroughFields(original: Transaction): Transaction =
    new Immutable(
      producerTransaction =
        {
          val field = original.producerTransaction
          field.map { field =>
            transactionService.rpc.ProducerTransaction.withoutPassthroughFields(field)
          }
        },
      consumerTransaction =
        {
          val field = original.consumerTransaction
          field.map { field =>
            transactionService.rpc.ConsumerTransaction.withoutPassthroughFields(field)
          }
        }
    )

  override def encode(_item: Transaction, _oproto: TProtocol): Unit = {
    _item.write(_oproto)
  }

  private[this] def lazyDecode(_iprot: LazyTProtocol): Transaction = {

    var producerTransaction: Option[transactionService.rpc.ProducerTransaction] = None
    var consumerTransaction: Option[transactionService.rpc.ConsumerTransaction] = None

    var _passthroughFields: Builder[(Short, TFieldBlob), immutable$Map[Short, TFieldBlob]] = null
    var _done = false
    val _start_offset = _iprot.offset

    _iprot.readStructBegin()
    while (!_done) {
      val _field = _iprot.readFieldBegin()
      if (_field.`type` == TType.STOP) {
        _done = true
      } else {
        _field.id match {
          case 1 =>
            _field.`type` match {
              case TType.STRUCT =>
    
                producerTransaction = Some(readProducerTransactionValue(_iprot))
              case _actualType =>
                val _expectedType = TType.STRUCT
                throw new TProtocolException(
                  "Received wrong type for field 'producerTransaction' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.STRUCT =>
    
                consumerTransaction = Some(readConsumerTransactionValue(_iprot))
              case _actualType =>
                val _expectedType = TType.STRUCT
                throw new TProtocolException(
                  "Received wrong type for field 'consumerTransaction' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case _ =>
            if (_passthroughFields == null)
              _passthroughFields = immutable$Map.newBuilder[Short, TFieldBlob]
            _passthroughFields += (_field.id -> TFieldBlob.read(_field, _iprot))
        }
        _iprot.readFieldEnd()
      }
    }
    _iprot.readStructEnd()

    new LazyImmutable(
      _iprot,
      _iprot.buffer,
      _start_offset,
      _iprot.offset,
      producerTransaction,
      consumerTransaction,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  override def decode(_iprot: TProtocol): Transaction =
    _iprot match {
      case i: LazyTProtocol => lazyDecode(i)
      case i => eagerDecode(i)
    }

  private[this] def eagerDecode(_iprot: TProtocol): Transaction = {
    var producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction] = _root_.scala.None
    var consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction] = _root_.scala.None
    var _passthroughFields: Builder[(Short, TFieldBlob), immutable$Map[Short, TFieldBlob]] = null
    var _done = false

    _iprot.readStructBegin()
    while (!_done) {
      val _field = _iprot.readFieldBegin()
      if (_field.`type` == TType.STOP) {
        _done = true
      } else {
        _field.id match {
          case 1 =>
            _field.`type` match {
              case TType.STRUCT =>
                producerTransaction = _root_.scala.Some(readProducerTransactionValue(_iprot))
              case _actualType =>
                val _expectedType = TType.STRUCT
                throw new TProtocolException(
                  "Received wrong type for field 'producerTransaction' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.STRUCT =>
                consumerTransaction = _root_.scala.Some(readConsumerTransactionValue(_iprot))
              case _actualType =>
                val _expectedType = TType.STRUCT
                throw new TProtocolException(
                  "Received wrong type for field 'consumerTransaction' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case _ =>
            if (_passthroughFields == null)
              _passthroughFields = immutable$Map.newBuilder[Short, TFieldBlob]
            _passthroughFields += (_field.id -> TFieldBlob.read(_field, _iprot))
        }
        _iprot.readFieldEnd()
      }
    }
    _iprot.readStructEnd()

    new Immutable(
      producerTransaction,
      consumerTransaction,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  def apply(
    producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction] = _root_.scala.None,
    consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction] = _root_.scala.None
  ): Transaction =
    new Immutable(
      producerTransaction,
      consumerTransaction
    )

  def unapply(_item: Transaction): _root_.scala.Option[_root_.scala.Tuple2[Option[transactionService.rpc.ProducerTransaction], Option[transactionService.rpc.ConsumerTransaction]]] = _root_.scala.Some(_item.toTuple)


  @inline private def readProducerTransactionValue(_iprot: TProtocol): transactionService.rpc.ProducerTransaction = {
    transactionService.rpc.ProducerTransaction.decode(_iprot)
  }

  @inline private def writeProducerTransactionField(producerTransaction_item: transactionService.rpc.ProducerTransaction, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(ProducerTransactionField)
    writeProducerTransactionValue(producerTransaction_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeProducerTransactionValue(producerTransaction_item: transactionService.rpc.ProducerTransaction, _oprot: TProtocol): Unit = {
    producerTransaction_item.write(_oprot)
  }

  @inline private def readConsumerTransactionValue(_iprot: TProtocol): transactionService.rpc.ConsumerTransaction = {
    transactionService.rpc.ConsumerTransaction.decode(_iprot)
  }

  @inline private def writeConsumerTransactionField(consumerTransaction_item: transactionService.rpc.ConsumerTransaction, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(ConsumerTransactionField)
    writeConsumerTransactionValue(consumerTransaction_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeConsumerTransactionValue(consumerTransaction_item: transactionService.rpc.ConsumerTransaction, _oprot: TProtocol): Unit = {
    consumerTransaction_item.write(_oprot)
  }


  object Immutable extends ThriftStructCodec3[Transaction] {
    override def encode(_item: Transaction, _oproto: TProtocol): Unit = { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): Transaction = Transaction.decode(_iprot)
    override lazy val metaData: ThriftStructMetaData[Transaction] = Transaction.metaData
  }

  /**
   * The default read-only implementation of Transaction.  You typically should not need to
   * directly reference this class; instead, use the Transaction.apply method to construct
   * new instances.
   */
  class Immutable(
      val producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction],
      val consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction],
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends Transaction {
    def this(
      producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction] = _root_.scala.None,
      consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction] = _root_.scala.None
    ) = this(
      producerTransaction,
      consumerTransaction,
      Map.empty
    )
  }

  /**
   * This is another Immutable, this however keeps strings as lazy values that are lazily decoded from the backing
   * array byte on read.
   */
  private[this] class LazyImmutable(
      _proto: LazyTProtocol,
      _buf: Array[Byte],
      _start_offset: Int,
      _end_offset: Int,
      val producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction],
      val consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction],
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends Transaction {

    override def write(_oprot: TProtocol): Unit = {
      _oprot match {
        case i: LazyTProtocol => i.writeRaw(_buf, _start_offset, _end_offset - _start_offset)
        case _ => super.write(_oprot)
      }
    }


    /**
     * Override the super hash code to make it a lazy val rather than def.
     *
     * Calculating the hash code can be expensive, caching it where possible
     * can provide significant performance wins. (Key in a hash map for instance)
     * Usually not safe since the normal constructor will accept a mutable map or
     * set as an arg
     * Here however we control how the class is generated from serialized data.
     * With the class private and the contract that we throw away our mutable references
     * having the hash code lazy here is safe.
     */
    override lazy val hashCode = super.hashCode
  }

  /**
   * This Proxy trait allows you to extend the Transaction trait with additional state or
   * behavior and implement the read-only methods from Transaction using an underlying
   * instance.
   */
  trait Proxy extends Transaction {
    protected def _underlying_Transaction: Transaction
    override def producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction] = _underlying_Transaction.producerTransaction
    override def consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction] = _underlying_Transaction.consumerTransaction
    override def _passthroughFields = _underlying_Transaction._passthroughFields
  }
}

trait Transaction
  extends ThriftStruct
  with _root_.scala.Product2[Option[transactionService.rpc.ProducerTransaction], Option[transactionService.rpc.ConsumerTransaction]]
  with HasThriftStructCodec3[Transaction]
  with java.io.Serializable
{
  import Transaction._

  def producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction]
  def consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction]

  def _passthroughFields: immutable$Map[Short, TFieldBlob] = immutable$Map.empty

  def _1 = producerTransaction
  def _2 = consumerTransaction

  def toTuple: _root_.scala.Tuple2[Option[transactionService.rpc.ProducerTransaction], Option[transactionService.rpc.ConsumerTransaction]] = {
    (
      producerTransaction,
      consumerTransaction
    )
  }


  /**
   * Gets a field value encoded as a binary blob using TCompactProtocol.  If the specified field
   * is present in the passthrough map, that value is returned.  Otherwise, if the specified field
   * is known and not optional and set to None, then the field is serialized and returned.
   */
  def getFieldBlob(_fieldId: Short): _root_.scala.Option[TFieldBlob] = {
    lazy val _buff = new TMemoryBuffer(32)
    lazy val _oprot = new TCompactProtocol(_buff)
    _passthroughFields.get(_fieldId) match {
      case blob: _root_.scala.Some[TFieldBlob] => blob
      case _root_.scala.None => {
        val _fieldOpt: _root_.scala.Option[TField] =
          _fieldId match {
            case 1 =>
              if (producerTransaction.isDefined) {
                writeProducerTransactionValue(producerTransaction.get, _oprot)
                _root_.scala.Some(Transaction.ProducerTransactionField)
              } else {
                _root_.scala.None
              }
            case 2 =>
              if (consumerTransaction.isDefined) {
                writeConsumerTransactionValue(consumerTransaction.get, _oprot)
                _root_.scala.Some(Transaction.ConsumerTransactionField)
              } else {
                _root_.scala.None
              }
            case _ => _root_.scala.None
          }
        _fieldOpt match {
          case _root_.scala.Some(_field) =>
            val _data = Arrays.copyOfRange(_buff.getArray, 0, _buff.length)
            _root_.scala.Some(TFieldBlob(_field, _data))
          case _root_.scala.None =>
            _root_.scala.None
        }
      }
    }
  }

  /**
   * Collects TCompactProtocol-encoded field values according to `getFieldBlob` into a map.
   */
  def getFieldBlobs(ids: TraversableOnce[Short]): immutable$Map[Short, TFieldBlob] =
    (ids flatMap { id => getFieldBlob(id) map { id -> _ } }).toMap

  /**
   * Sets a field using a TCompactProtocol-encoded binary blob.  If the field is a known
   * field, the blob is decoded and the field is set to the decoded value.  If the field
   * is unknown and passthrough fields are enabled, then the blob will be stored in
   * _passthroughFields.
   */
  def setField(_blob: TFieldBlob): Transaction = {
    var producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction] = this.producerTransaction
    var consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction] = this.consumerTransaction
    var _passthroughFields = this._passthroughFields
    _blob.id match {
      case 1 =>
        producerTransaction = _root_.scala.Some(readProducerTransactionValue(_blob.read))
      case 2 =>
        consumerTransaction = _root_.scala.Some(readConsumerTransactionValue(_blob.read))
      case _ => _passthroughFields += (_blob.id -> _blob)
    }
    new Immutable(
      producerTransaction,
      consumerTransaction,
      _passthroughFields
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetField(_fieldId: Short): Transaction = {
    var producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction] = this.producerTransaction
    var consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction] = this.consumerTransaction

    _fieldId match {
      case 1 =>
        producerTransaction = _root_.scala.None
      case 2 =>
        consumerTransaction = _root_.scala.None
      case _ =>
    }
    new Immutable(
      producerTransaction,
      consumerTransaction,
      _passthroughFields - _fieldId
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetProducerTransaction: Transaction = unsetField(1)

  def unsetConsumerTransaction: Transaction = unsetField(2)


  override def write(_oprot: TProtocol): Unit = {
    Transaction.validate(this)
    _oprot.writeStructBegin(Struct)
    if (producerTransaction.isDefined) writeProducerTransactionField(producerTransaction.get, _oprot)
    if (consumerTransaction.isDefined) writeConsumerTransactionField(consumerTransaction.get, _oprot)
    if (_passthroughFields.nonEmpty) {
      _passthroughFields.values.foreach { _.write(_oprot) }
    }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    producerTransaction: _root_.scala.Option[transactionService.rpc.ProducerTransaction] = this.producerTransaction,
    consumerTransaction: _root_.scala.Option[transactionService.rpc.ConsumerTransaction] = this.consumerTransaction,
    _passthroughFields: immutable$Map[Short, TFieldBlob] = this._passthroughFields
  ): Transaction =
    new Immutable(
      producerTransaction,
      consumerTransaction,
      _passthroughFields
    )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[Transaction]

  private def _equals(x: Transaction, y: Transaction): Boolean =
      x.productArity == y.productArity &&
      x.productIterator.sameElements(y.productIterator)

  override def equals(other: Any): Boolean =
    canEqual(other) &&
      _equals(this, other.asInstanceOf[Transaction]) &&
      _passthroughFields == other.asInstanceOf[Transaction]._passthroughFields

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)


  override def productArity: Int = 2

  override def productElement(n: Int): Any = n match {
    case 0 => this.producerTransaction
    case 1 => this.consumerTransaction
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "Transaction"

  def _codec: ThriftStructCodec3[Transaction] = Transaction
}