/**
 * Generated by Scrooge
 *   version: 4.11.0
 *   rev: 53125d3304736db41e23d8474e5765d6c2fe3ded
 *   built at: 20161011-151232
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


object ProducerTransaction extends ThriftStructCodec3[ProducerTransaction] {
  private val NoPassthroughFields = immutable$Map.empty[Short, TFieldBlob]
  val Struct = new TStruct("ProducerTransaction")
  val StreamField = new TField("stream", TType.STRING, 1)
  val StreamFieldManifest = implicitly[Manifest[String]]
  val PartitionField = new TField("partition", TType.I32, 2)
  val PartitionFieldManifest = implicitly[Manifest[Int]]
  val TransactionIDField = new TField("transactionID", TType.I64, 3)
  val TransactionIDFieldManifest = implicitly[Manifest[Long]]
  val StateField = new TField("state", TType.ENUM, 4)
  val StateFieldI32 = new TField("state", TType.I32, 4)
  val StateFieldManifest = implicitly[Manifest[transactionService.rpc.TransactionStates]]
  val QuantityField = new TField("quantity", TType.I32, 5)
  val QuantityFieldManifest = implicitly[Manifest[Int]]
  val TimestampField = new TField("timestamp", TType.I64, 6)
  val TimestampFieldManifest = implicitly[Manifest[Long]]

  /**
   * Field information in declaration order.
   */
  lazy val fieldInfos: scala.List[ThriftStructFieldInfo] = scala.List[ThriftStructFieldInfo](
    new ThriftStructFieldInfo(
      StreamField,
      false,
      true,
      StreamFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      PartitionField,
      false,
      true,
      PartitionFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      TransactionIDField,
      false,
      true,
      TransactionIDFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      StateField,
      false,
      true,
      StateFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      QuantityField,
      false,
      true,
      QuantityFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      TimestampField,
      false,
      true,
      TimestampFieldManifest,
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
  def validate(_item: ProducerTransaction): Unit = {
    if (_item.stream == null) throw new TProtocolException("Required field stream cannot be null")
    if (_item.state == null) throw new TProtocolException("Required field state cannot be null")
  }

  def withoutPassthroughFields(original: ProducerTransaction): ProducerTransaction =
    new Immutable(
      stream =
        {
          val field = original.stream
          field
        },
      partition =
        {
          val field = original.partition
          field
        },
      transactionID =
        {
          val field = original.transactionID
          field
        },
      state =
        {
          val field = original.state
          field
        },
      quantity =
        {
          val field = original.quantity
          field
        },
      timestamp =
        {
          val field = original.timestamp
          field
        }
    )

  override def encode(_item: ProducerTransaction, _oproto: TProtocol): Unit = {
    _item.write(_oproto)
  }

  private[this] def lazyDecode(_iprot: LazyTProtocol): ProducerTransaction = {

    var streamOffset: Int = -1
    var _got_stream = false
    var partition: Int = 0
    var _got_partition = false
    var transactionID: Long = 0L
    var _got_transactionID = false
    var state: transactionService.rpc.TransactionStates = null
    var _got_state = false
    var quantity: Int = 0
    var _got_quantity = false
    var timestamp: Long = 0L
    var _got_timestamp = false

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
              case TType.STRING =>
                streamOffset = _iprot.offsetSkipString
    
                _got_stream = true
              case _actualType =>
                val _expectedType = TType.STRING
                throw new TProtocolException(
                  "Received wrong type for field 'stream' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.I32 =>
    
                partition = readPartitionValue(_iprot)
                _got_partition = true
              case _actualType =>
                val _expectedType = TType.I32
                throw new TProtocolException(
                  "Received wrong type for field 'partition' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 3 =>
            _field.`type` match {
              case TType.I64 =>
    
                transactionID = readTransactionIDValue(_iprot)
                _got_transactionID = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'transactionID' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 4 =>
            _field.`type` match {
              case TType.I32 | TType.ENUM =>
    
                state = readStateValue(_iprot)
                _got_state = true
              case _actualType =>
                val _expectedType = TType.ENUM
                throw new TProtocolException(
                  "Received wrong type for field 'state' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 5 =>
            _field.`type` match {
              case TType.I32 =>
    
                quantity = readQuantityValue(_iprot)
                _got_quantity = true
              case _actualType =>
                val _expectedType = TType.I32
                throw new TProtocolException(
                  "Received wrong type for field 'quantity' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 6 =>
            _field.`type` match {
              case TType.I64 =>
    
                timestamp = readTimestampValue(_iprot)
                _got_timestamp = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'timestamp' (expected=%s, actual=%s).".format(
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

    if (!_got_stream) throw new TProtocolException("Required field 'stream' was not found in serialized data for struct ProducerTransaction")
    if (!_got_partition) throw new TProtocolException("Required field 'partition' was not found in serialized data for struct ProducerTransaction")
    if (!_got_transactionID) throw new TProtocolException("Required field 'transactionID' was not found in serialized data for struct ProducerTransaction")
    if (!_got_state) throw new TProtocolException("Required field 'state' was not found in serialized data for struct ProducerTransaction")
    if (!_got_quantity) throw new TProtocolException("Required field 'quantity' was not found in serialized data for struct ProducerTransaction")
    if (!_got_timestamp) throw new TProtocolException("Required field 'timestamp' was not found in serialized data for struct ProducerTransaction")
    new LazyImmutable(
      _iprot,
      _iprot.buffer,
      _start_offset,
      _iprot.offset,
      streamOffset,
      partition,
      transactionID,
      state,
      quantity,
      timestamp,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  override def decode(_iprot: TProtocol): ProducerTransaction =
    _iprot match {
      case i: LazyTProtocol => lazyDecode(i)
      case i => eagerDecode(i)
    }

  private[this] def eagerDecode(_iprot: TProtocol): ProducerTransaction = {
    var stream: String = null
    var _got_stream = false
    var partition: Int = 0
    var _got_partition = false
    var transactionID: Long = 0L
    var _got_transactionID = false
    var state: transactionService.rpc.TransactionStates = null
    var _got_state = false
    var quantity: Int = 0
    var _got_quantity = false
    var timestamp: Long = 0L
    var _got_timestamp = false
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
              case TType.STRING =>
                stream = readStreamValue(_iprot)
                _got_stream = true
              case _actualType =>
                val _expectedType = TType.STRING
                throw new TProtocolException(
                  "Received wrong type for field 'stream' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.I32 =>
                partition = readPartitionValue(_iprot)
                _got_partition = true
              case _actualType =>
                val _expectedType = TType.I32
                throw new TProtocolException(
                  "Received wrong type for field 'partition' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 3 =>
            _field.`type` match {
              case TType.I64 =>
                transactionID = readTransactionIDValue(_iprot)
                _got_transactionID = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'transactionID' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 4 =>
            _field.`type` match {
              case TType.I32 | TType.ENUM =>
                state = readStateValue(_iprot)
                _got_state = true
              case _actualType =>
                val _expectedType = TType.ENUM
                throw new TProtocolException(
                  "Received wrong type for field 'state' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 5 =>
            _field.`type` match {
              case TType.I32 =>
                quantity = readQuantityValue(_iprot)
                _got_quantity = true
              case _actualType =>
                val _expectedType = TType.I32
                throw new TProtocolException(
                  "Received wrong type for field 'quantity' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 6 =>
            _field.`type` match {
              case TType.I64 =>
                timestamp = readTimestampValue(_iprot)
                _got_timestamp = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'timestamp' (expected=%s, actual=%s).".format(
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

    if (!_got_stream) throw new TProtocolException("Required field 'stream' was not found in serialized data for struct ProducerTransaction")
    if (!_got_partition) throw new TProtocolException("Required field 'partition' was not found in serialized data for struct ProducerTransaction")
    if (!_got_transactionID) throw new TProtocolException("Required field 'transactionID' was not found in serialized data for struct ProducerTransaction")
    if (!_got_state) throw new TProtocolException("Required field 'state' was not found in serialized data for struct ProducerTransaction")
    if (!_got_quantity) throw new TProtocolException("Required field 'quantity' was not found in serialized data for struct ProducerTransaction")
    if (!_got_timestamp) throw new TProtocolException("Required field 'timestamp' was not found in serialized data for struct ProducerTransaction")
    new Immutable(
      stream,
      partition,
      transactionID,
      state,
      quantity,
      timestamp,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  def apply(
    stream: String,
    partition: Int,
    transactionID: Long,
    state: transactionService.rpc.TransactionStates,
    quantity: Int,
    timestamp: Long
  ): ProducerTransaction =
    new Immutable(
      stream,
      partition,
      transactionID,
      state,
      quantity,
      timestamp
    )

  def unapply(_item: ProducerTransaction): _root_.scala.Option[scala.Product6[String, Int, Long, transactionService.rpc.TransactionStates, Int, Long]] = _root_.scala.Some(_item)


  @inline private def readStreamValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  @inline private def writeStreamField(stream_item: String, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(StreamField)
    writeStreamValue(stream_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeStreamValue(stream_item: String, _oprot: TProtocol): Unit = {
    _oprot.writeString(stream_item)
  }

  @inline private def readPartitionValue(_iprot: TProtocol): Int = {
    _iprot.readI32()
  }

  @inline private def writePartitionField(partition_item: Int, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(PartitionField)
    writePartitionValue(partition_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writePartitionValue(partition_item: Int, _oprot: TProtocol): Unit = {
    _oprot.writeI32(partition_item)
  }

  @inline private def readTransactionIDValue(_iprot: TProtocol): Long = {
    _iprot.readI64()
  }

  @inline private def writeTransactionIDField(transactionID_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(TransactionIDField)
    writeTransactionIDValue(transactionID_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeTransactionIDValue(transactionID_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeI64(transactionID_item)
  }

  @inline private def readStateValue(_iprot: TProtocol): transactionService.rpc.TransactionStates = {
    transactionService.rpc.TransactionStates.getOrUnknown(_iprot.readI32())
  }

  @inline private def writeStateField(state_item: transactionService.rpc.TransactionStates, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(StateFieldI32)
    writeStateValue(state_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeStateValue(state_item: transactionService.rpc.TransactionStates, _oprot: TProtocol): Unit = {
    _oprot.writeI32(state_item.value)
  }

  @inline private def readQuantityValue(_iprot: TProtocol): Int = {
    _iprot.readI32()
  }

  @inline private def writeQuantityField(quantity_item: Int, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(QuantityField)
    writeQuantityValue(quantity_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeQuantityValue(quantity_item: Int, _oprot: TProtocol): Unit = {
    _oprot.writeI32(quantity_item)
  }

  @inline private def readTimestampValue(_iprot: TProtocol): Long = {
    _iprot.readI64()
  }

  @inline private def writeTimestampField(timestamp_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(TimestampField)
    writeTimestampValue(timestamp_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeTimestampValue(timestamp_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeI64(timestamp_item)
  }


  object Immutable extends ThriftStructCodec3[ProducerTransaction] {
    override def encode(_item: ProducerTransaction, _oproto: TProtocol): Unit = { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): ProducerTransaction = ProducerTransaction.decode(_iprot)
    override lazy val metaData: ThriftStructMetaData[ProducerTransaction] = ProducerTransaction.metaData
  }

  /**
   * The default read-only implementation of ProducerTransaction.  You typically should not need to
   * directly reference this class; instead, use the ProducerTransaction.apply method to construct
   * new instances.
   */
  class Immutable(
      val stream: String,
      val partition: Int,
      val transactionID: Long,
      val state: transactionService.rpc.TransactionStates,
      val quantity: Int,
      val timestamp: Long,
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends ProducerTransaction {
    def this(
      stream: String,
      partition: Int,
      transactionID: Long,
      state: transactionService.rpc.TransactionStates,
      quantity: Int,
      timestamp: Long
    ) = this(
      stream,
      partition,
      transactionID,
      state,
      quantity,
      timestamp,
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
      streamOffset: Int,
      val partition: Int,
      val transactionID: Long,
      val state: transactionService.rpc.TransactionStates,
      val quantity: Int,
      val timestamp: Long,
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends ProducerTransaction {

    override def write(_oprot: TProtocol): Unit = {
      _oprot match {
        case i: LazyTProtocol => i.writeRaw(_buf, _start_offset, _end_offset - _start_offset)
        case _ => super.write(_oprot)
      }
    }

    lazy val stream: String =
      if (streamOffset == -1)
        null
      else {
        _proto.decodeString(_buf, streamOffset)
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
   * This Proxy trait allows you to extend the ProducerTransaction trait with additional state or
   * behavior and implement the read-only methods from ProducerTransaction using an underlying
   * instance.
   */
  trait Proxy extends ProducerTransaction {
    protected def _underlying_ProducerTransaction: ProducerTransaction
    override def stream: String = _underlying_ProducerTransaction.stream
    override def partition: Int = _underlying_ProducerTransaction.partition
    override def transactionID: Long = _underlying_ProducerTransaction.transactionID
    override def state: transactionService.rpc.TransactionStates = _underlying_ProducerTransaction.state
    override def quantity: Int = _underlying_ProducerTransaction.quantity
    override def timestamp: Long = _underlying_ProducerTransaction.timestamp
    override def _passthroughFields = _underlying_ProducerTransaction._passthroughFields
  }
}

trait ProducerTransaction
  extends ThriftStruct
  with scala.Product6[String, Int, Long, transactionService.rpc.TransactionStates, Int, Long]
  with HasThriftStructCodec3[ProducerTransaction]
  with java.io.Serializable
{
  import ProducerTransaction._

  def stream: String
  def partition: Int
  def transactionID: Long
  def state: transactionService.rpc.TransactionStates
  def quantity: Int
  def timestamp: Long

  def _passthroughFields: immutable$Map[Short, TFieldBlob] = immutable$Map.empty

  def _1 = stream
  def _2 = partition
  def _3 = transactionID
  def _4 = state
  def _5 = quantity
  def _6 = timestamp


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
              if (stream ne null) {
                writeStreamValue(stream, _oprot)
                _root_.scala.Some(ProducerTransaction.StreamField)
              } else {
                _root_.scala.None
              }
            case 2 =>
              if (true) {
                writePartitionValue(partition, _oprot)
                _root_.scala.Some(ProducerTransaction.PartitionField)
              } else {
                _root_.scala.None
              }
            case 3 =>
              if (true) {
                writeTransactionIDValue(transactionID, _oprot)
                _root_.scala.Some(ProducerTransaction.TransactionIDField)
              } else {
                _root_.scala.None
              }
            case 4 =>
              if (state ne null) {
                writeStateValue(state, _oprot)
                _root_.scala.Some(ProducerTransaction.StateField)
              } else {
                _root_.scala.None
              }
            case 5 =>
              if (true) {
                writeQuantityValue(quantity, _oprot)
                _root_.scala.Some(ProducerTransaction.QuantityField)
              } else {
                _root_.scala.None
              }
            case 6 =>
              if (true) {
                writeTimestampValue(timestamp, _oprot)
                _root_.scala.Some(ProducerTransaction.TimestampField)
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
  def setField(_blob: TFieldBlob): ProducerTransaction = {
    var stream: String = this.stream
    var partition: Int = this.partition
    var transactionID: Long = this.transactionID
    var state: transactionService.rpc.TransactionStates = this.state
    var quantity: Int = this.quantity
    var timestamp: Long = this.timestamp
    var _passthroughFields = this._passthroughFields
    _blob.id match {
      case 1 =>
        stream = readStreamValue(_blob.read)
      case 2 =>
        partition = readPartitionValue(_blob.read)
      case 3 =>
        transactionID = readTransactionIDValue(_blob.read)
      case 4 =>
        state = readStateValue(_blob.read)
      case 5 =>
        quantity = readQuantityValue(_blob.read)
      case 6 =>
        timestamp = readTimestampValue(_blob.read)
      case _ => _passthroughFields += (_blob.id -> _blob)
    }
    new Immutable(
      stream,
      partition,
      transactionID,
      state,
      quantity,
      timestamp,
      _passthroughFields
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetField(_fieldId: Short): ProducerTransaction = {
    var stream: String = this.stream
    var partition: Int = this.partition
    var transactionID: Long = this.transactionID
    var state: transactionService.rpc.TransactionStates = this.state
    var quantity: Int = this.quantity
    var timestamp: Long = this.timestamp

    _fieldId match {
      case 1 =>
        stream = null
      case 2 =>
        partition = 0
      case 3 =>
        transactionID = 0L
      case 4 =>
        state = null
      case 5 =>
        quantity = 0
      case 6 =>
        timestamp = 0L
      case _ =>
    }
    new Immutable(
      stream,
      partition,
      transactionID,
      state,
      quantity,
      timestamp,
      _passthroughFields - _fieldId
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetStream: ProducerTransaction = unsetField(1)

  def unsetPartition: ProducerTransaction = unsetField(2)

  def unsetTransactionID: ProducerTransaction = unsetField(3)

  def unsetState: ProducerTransaction = unsetField(4)

  def unsetQuantity: ProducerTransaction = unsetField(5)

  def unsetTimestamp: ProducerTransaction = unsetField(6)


  override def write(_oprot: TProtocol): Unit = {
    ProducerTransaction.validate(this)
    _oprot.writeStructBegin(Struct)
    if (stream ne null) writeStreamField(stream, _oprot)
    writePartitionField(partition, _oprot)
    writeTransactionIDField(transactionID, _oprot)
    if (state ne null) writeStateField(state, _oprot)
    writeQuantityField(quantity, _oprot)
    writeTimestampField(timestamp, _oprot)
    if (_passthroughFields.nonEmpty) {
      _passthroughFields.values.foreach { _.write(_oprot) }
    }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    stream: String = this.stream,
    partition: Int = this.partition,
    transactionID: Long = this.transactionID,
    state: transactionService.rpc.TransactionStates = this.state,
    quantity: Int = this.quantity,
    timestamp: Long = this.timestamp,
    _passthroughFields: immutable$Map[Short, TFieldBlob] = this._passthroughFields
  ): ProducerTransaction =
    new Immutable(
      stream,
      partition,
      transactionID,
      state,
      quantity,
      timestamp,
      _passthroughFields
    )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ProducerTransaction]

  override def equals(other: Any): Boolean =
    canEqual(other) &&
      _root_.scala.runtime.ScalaRunTime._equals(this, other) &&
      _passthroughFields == other.asInstanceOf[ProducerTransaction]._passthroughFields

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)


  override def productArity: Int = 6

  override def productElement(n: Int): Any = n match {
    case 0 => this.stream
    case 1 => this.partition
    case 2 => this.transactionID
    case 3 => this.state
    case 4 => this.quantity
    case 5 => this.timestamp
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "ProducerTransaction"

  def _codec: ThriftStructCodec3[ProducerTransaction] = ProducerTransaction
}