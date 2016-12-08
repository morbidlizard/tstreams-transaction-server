/**
 * Generated by Scrooge
 *   version: 4.12.0
 *   rev: f7190e7f6b92684107b8cebf853d0d2403473022
 *   built at: 20161122-154730
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


object Stream extends ThriftStructCodec3[Stream] {
  private val NoPassthroughFields = immutable$Map.empty[Short, TFieldBlob]
  val Struct = new TStruct("Stream")
  val NameField = new TField("name", TType.STRING, 1)
  val NameFieldManifest = implicitly[Manifest[String]]
  val PartitionsField = new TField("partitions", TType.I32, 2)
  val PartitionsFieldManifest = implicitly[Manifest[Int]]
  val DescriptionField = new TField("description", TType.STRING, 3)
  val DescriptionFieldManifest = implicitly[Manifest[String]]
  val TtlField = new TField("ttl", TType.I32, 4)
  val TtlFieldManifest = implicitly[Manifest[Int]]

  /**
   * Field information in declaration order.
   */
  lazy val fieldInfos: scala.List[ThriftStructFieldInfo] = scala.List[ThriftStructFieldInfo](
    new ThriftStructFieldInfo(
      NameField,
      false,
      true,
      NameFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      PartitionsField,
      false,
      true,
      PartitionsFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      DescriptionField,
      true,
      false,
      DescriptionFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      TtlField,
      false,
      true,
      TtlFieldManifest,
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
  def validate(_item: Stream): Unit = {
    if (_item.name == null) throw new TProtocolException("Required field name cannot be null")
  }

  def withoutPassthroughFields(original: Stream): Stream =
    new Immutable(
      name =
        {
          val field = original.name
          field
        },
      partitions =
        {
          val field = original.partitions
          field
        },
      description =
        {
          val field = original.description
          field.map { field =>
            field
          }
        },
      ttl =
        {
          val field = original.ttl
          field
        }
    )

  override def encode(_item: Stream, _oproto: TProtocol): Unit = {
    _item.write(_oproto)
  }

  private[this] def lazyDecode(_iprot: LazyTProtocol): Stream = {

    var nameOffset: Int = -1
    var _got_name = false
    var partitions: Int = 0
    var _got_partitions = false
    var descriptionOffset: Int = -1
    var ttl: Int = 0
    var _got_ttl = false

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
                nameOffset = _iprot.offsetSkipString
    
                _got_name = true
              case _actualType =>
                val _expectedType = TType.STRING
                throw new TProtocolException(
                  "Received wrong type for field 'name' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.I32 =>
    
                partitions = readPartitionsValue(_iprot)
                _got_partitions = true
              case _actualType =>
                val _expectedType = TType.I32
                throw new TProtocolException(
                  "Received wrong type for field 'partitions' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 3 =>
            _field.`type` match {
              case TType.STRING =>
                descriptionOffset = _iprot.offsetSkipString
    
              case _actualType =>
                val _expectedType = TType.STRING
                throw new TProtocolException(
                  "Received wrong type for field 'description' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 4 =>
            _field.`type` match {
              case TType.I32 =>
    
                ttl = readTtlValue(_iprot)
                _got_ttl = true
              case _actualType =>
                val _expectedType = TType.I32
                throw new TProtocolException(
                  "Received wrong type for field 'ttl' (expected=%s, actual=%s).".format(
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

    if (!_got_name) throw new TProtocolException("Required field 'name' was not found in serialized data for struct Stream")
    if (!_got_partitions) throw new TProtocolException("Required field 'partitions' was not found in serialized data for struct Stream")
    if (!_got_ttl) throw new TProtocolException("Required field 'ttl' was not found in serialized data for struct Stream")
    new LazyImmutable(
      _iprot,
      _iprot.buffer,
      _start_offset,
      _iprot.offset,
      nameOffset,
      partitions,
      descriptionOffset,
      ttl,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  override def decode(_iprot: TProtocol): Stream =
    _iprot match {
      case i: LazyTProtocol => lazyDecode(i)
      case i => eagerDecode(i)
    }

  private[this] def eagerDecode(_iprot: TProtocol): Stream = {
    var name: String = null
    var _got_name = false
    var partitions: Int = 0
    var _got_partitions = false
    var description: _root_.scala.Option[String] = _root_.scala.None
    var ttl: Int = 0
    var _got_ttl = false
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
                name = readNameValue(_iprot)
                _got_name = true
              case _actualType =>
                val _expectedType = TType.STRING
                throw new TProtocolException(
                  "Received wrong type for field 'name' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.I32 =>
                partitions = readPartitionsValue(_iprot)
                _got_partitions = true
              case _actualType =>
                val _expectedType = TType.I32
                throw new TProtocolException(
                  "Received wrong type for field 'partitions' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 3 =>
            _field.`type` match {
              case TType.STRING =>
                description = _root_.scala.Some(readDescriptionValue(_iprot))
              case _actualType =>
                val _expectedType = TType.STRING
                throw new TProtocolException(
                  "Received wrong type for field 'description' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 4 =>
            _field.`type` match {
              case TType.I32 =>
                ttl = readTtlValue(_iprot)
                _got_ttl = true
              case _actualType =>
                val _expectedType = TType.I32
                throw new TProtocolException(
                  "Received wrong type for field 'ttl' (expected=%s, actual=%s).".format(
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

    if (!_got_name) throw new TProtocolException("Required field 'name' was not found in serialized data for struct Stream")
    if (!_got_partitions) throw new TProtocolException("Required field 'partitions' was not found in serialized data for struct Stream")
    if (!_got_ttl) throw new TProtocolException("Required field 'ttl' was not found in serialized data for struct Stream")
    new Immutable(
      name,
      partitions,
      description,
      ttl,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  def apply(
    name: String,
    partitions: Int,
    description: _root_.scala.Option[String] = _root_.scala.None,
    ttl: Int
  ): Stream =
    new Immutable(
      name,
      partitions,
      description,
      ttl
    )

  def unapply(_item: Stream): _root_.scala.Option[scala.Product4[String, Int, Option[String], Int]] = _root_.scala.Some(_item)


  @inline private def readNameValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  @inline private def writeNameField(name_item: String, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(NameField)
    writeNameValue(name_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeNameValue(name_item: String, _oprot: TProtocol): Unit = {
    _oprot.writeString(name_item)
  }

  @inline private def readPartitionsValue(_iprot: TProtocol): Int = {
    _iprot.readI32()
  }

  @inline private def writePartitionsField(partitions_item: Int, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(PartitionsField)
    writePartitionsValue(partitions_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writePartitionsValue(partitions_item: Int, _oprot: TProtocol): Unit = {
    _oprot.writeI32(partitions_item)
  }

  @inline private def readDescriptionValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  @inline private def writeDescriptionField(description_item: String, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(DescriptionField)
    writeDescriptionValue(description_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeDescriptionValue(description_item: String, _oprot: TProtocol): Unit = {
    _oprot.writeString(description_item)
  }

  @inline private def readTtlValue(_iprot: TProtocol): Int = {
    _iprot.readI32()
  }

  @inline private def writeTtlField(ttl_item: Int, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(TtlField)
    writeTtlValue(ttl_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeTtlValue(ttl_item: Int, _oprot: TProtocol): Unit = {
    _oprot.writeI32(ttl_item)
  }


  object Immutable extends ThriftStructCodec3[Stream] {
    override def encode(_item: Stream, _oproto: TProtocol): Unit = { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): Stream = Stream.decode(_iprot)
    override lazy val metaData: ThriftStructMetaData[Stream] = Stream.metaData
  }

  /**
   * The default read-only implementation of Stream.  You typically should not need to
   * directly reference this class; instead, use the Stream.apply method to construct
   * new instances.
   */
  class Immutable(
      val name: String,
      val partitions: Int,
      val description: _root_.scala.Option[String],
      val ttl: Int,
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends Stream {
    def this(
      name: String,
      partitions: Int,
      description: _root_.scala.Option[String] = _root_.scala.None,
      ttl: Int
    ) = this(
      name,
      partitions,
      description,
      ttl,
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
      nameOffset: Int,
      val partitions: Int,
      descriptionOffset: Int,
      val ttl: Int,
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends Stream {

    override def write(_oprot: TProtocol): Unit = {
      _oprot match {
        case i: LazyTProtocol => i.writeRaw(_buf, _start_offset, _end_offset - _start_offset)
        case _ => super.write(_oprot)
      }
    }

    lazy val name: String =
      if (nameOffset == -1)
        null
      else {
        _proto.decodeString(_buf, nameOffset)
      }
    lazy val description: _root_.scala.Option[String] =
      if (descriptionOffset == -1)
        None
      else {
        Some(_proto.decodeString(_buf, descriptionOffset))
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
   * This Proxy trait allows you to extend the Stream trait with additional state or
   * behavior and implement the read-only methods from Stream using an underlying
   * instance.
   */
  trait Proxy extends Stream {
    protected def _underlying_Stream: Stream
    override def name: String = _underlying_Stream.name
    override def partitions: Int = _underlying_Stream.partitions
    override def description: _root_.scala.Option[String] = _underlying_Stream.description
    override def ttl: Int = _underlying_Stream.ttl
    override def _passthroughFields = _underlying_Stream._passthroughFields
  }
}

trait Stream
  extends ThriftStruct
  with scala.Product4[String, Int, Option[String], Int]
  with HasThriftStructCodec3[Stream]
  with java.io.Serializable
{
  import Stream._

  def name: String
  def partitions: Int
  def description: _root_.scala.Option[String]
  def ttl: Int

  def _passthroughFields: immutable$Map[Short, TFieldBlob] = immutable$Map.empty

  def _1 = name
  def _2 = partitions
  def _3 = description
  def _4 = ttl


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
              if (name ne null) {
                writeNameValue(name, _oprot)
                _root_.scala.Some(Stream.NameField)
              } else {
                _root_.scala.None
              }
            case 2 =>
              if (true) {
                writePartitionsValue(partitions, _oprot)
                _root_.scala.Some(Stream.PartitionsField)
              } else {
                _root_.scala.None
              }
            case 3 =>
              if (description.isDefined) {
                writeDescriptionValue(description.get, _oprot)
                _root_.scala.Some(Stream.DescriptionField)
              } else {
                _root_.scala.None
              }
            case 4 =>
              if (true) {
                writeTtlValue(ttl, _oprot)
                _root_.scala.Some(Stream.TtlField)
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
  def setField(_blob: TFieldBlob): Stream = {
    var name: String = this.name
    var partitions: Int = this.partitions
    var description: _root_.scala.Option[String] = this.description
    var ttl: Int = this.ttl
    var _passthroughFields = this._passthroughFields
    _blob.id match {
      case 1 =>
        name = readNameValue(_blob.read)
      case 2 =>
        partitions = readPartitionsValue(_blob.read)
      case 3 =>
        description = _root_.scala.Some(readDescriptionValue(_blob.read))
      case 4 =>
        ttl = readTtlValue(_blob.read)
      case _ => _passthroughFields += (_blob.id -> _blob)
    }
    new Immutable(
      name,
      partitions,
      description,
      ttl,
      _passthroughFields
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetField(_fieldId: Short): Stream = {
    var name: String = this.name
    var partitions: Int = this.partitions
    var description: _root_.scala.Option[String] = this.description
    var ttl: Int = this.ttl

    _fieldId match {
      case 1 =>
        name = null
      case 2 =>
        partitions = 0
      case 3 =>
        description = _root_.scala.None
      case 4 =>
        ttl = 0
      case _ =>
    }
    new Immutable(
      name,
      partitions,
      description,
      ttl,
      _passthroughFields - _fieldId
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetName: Stream = unsetField(1)

  def unsetPartitions: Stream = unsetField(2)

  def unsetDescription: Stream = unsetField(3)

  def unsetTtl: Stream = unsetField(4)


  override def write(_oprot: TProtocol): Unit = {
    Stream.validate(this)
    _oprot.writeStructBegin(Struct)
    if (name ne null) writeNameField(name, _oprot)
    writePartitionsField(partitions, _oprot)
    if (description.isDefined) writeDescriptionField(description.get, _oprot)
    writeTtlField(ttl, _oprot)
    if (_passthroughFields.nonEmpty) {
      _passthroughFields.values.foreach { _.write(_oprot) }
    }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    name: String = this.name,
    partitions: Int = this.partitions,
    description: _root_.scala.Option[String] = this.description,
    ttl: Int = this.ttl,
    _passthroughFields: immutable$Map[Short, TFieldBlob] = this._passthroughFields
  ): Stream =
    new Immutable(
      name,
      partitions,
      description,
      ttl,
      _passthroughFields
    )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[Stream]

  private def _equals(x: Stream, y: Stream): Boolean =
      x.productArity == y.productArity &&
      x.productIterator.sameElements(y.productIterator)

  override def equals(other: Any): Boolean =
    canEqual(other) &&
      _equals(this, other.asInstanceOf[Stream]) &&
      _passthroughFields == other.asInstanceOf[Stream]._passthroughFields

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)


  override def productArity: Int = 4

  override def productElement(n: Int): Any = n match {
    case 0 => this.name
    case 1 => this.partitions
    case 2 => this.description
    case 3 => this.ttl
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "Stream"

  def _codec: ThriftStructCodec3[Stream] = Stream
}