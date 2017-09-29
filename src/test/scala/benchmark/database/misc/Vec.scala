package benchmark.database.misc

import scala.language.implicitConversions

object ArithmeticImplicits {

  implicit def vec2arithmetic(value: Vec): Arithmetic[Vec] =
    new VecArithmetic(value:_*)

  implicit def arithmetic2vec(value: Arithmetic[Vec]): Vec =
    new Vec((value + new Vec()):_*)

  implicit def double2arithmetic(value: Double): Arithmetic[Double] =
    new DoubleArithmetic(value)

  implicit def arithmetic2double(value: Arithmetic[Double]): Double =
    value + 0d
}

trait Arithmetic[T] {
  def +(that: T): T
  def -(that: T): T
  def *(that: T): T
  def /(that: T): T
  def *(that: Double): T
  def /(that: Double): T
  def **(that: Double): T
}

class VecArithmetic(collection: Double*)
  extends Vec(collection:_*) with Arithmetic[Vec]

class DoubleArithmetic(val value: Double)
  extends Arithmetic[Double]{
  override def +(that: Double) = value + that

  override def -(that: Double) = value - that

  override def *(that: Double) = value * that

  override def /(that: Double) = value / that

  override def **(that: Double) = math.pow(value, that)
}

class Vec(private val collection: Double*)
  extends IndexedSeq[Double]
{

  def vecOp(that: Vec, op: (Double, Double) => Double): Vec =
    new Vec(
      this.zipAll(that, 0d, 0d)
        .map{
          case (x:Double, y:Double) => op(x,y)
        }:_*
    )

  def doubleOp(that: Double, op: (Double, Double) => Double): Vec =
    vecOp(new Vec(Array.fill(this.collection.length){that}:_*), op)

  def +(that: Vec): Vec =
    vecOp(that, (x, y) => x + y)

  def -(that: Vec): Vec =
    vecOp(that, (x, y) => x - y)

  def *(that: Vec): Vec =
    vecOp(that, (x, y) => x * y)

  def /(that: Vec): Vec =
    vecOp(that, (x, y) => x / y)

  def *(that: Double): Vec =
    doubleOp(that, (x, y) => x * y)

  def /(that: Double): Vec =
    doubleOp(that, (x, y) => x / y)

  def **(that: Double): Vec =
    doubleOp(that, (x, y) => math.pow(x, y))

  override val length = collection.length
  override def apply(idx: Int) = collection(idx)
}
