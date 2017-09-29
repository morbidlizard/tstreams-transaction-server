package benchmark.database.misc

import benchmark.database.misc.ArithmeticImplicits._
import org.scalatest.{FlatSpec, Matchers}

import scala.language.implicitConversions

class VecSpec extends FlatSpec with Matchers {
  "A vec" should "be empty" in {
    new Vec().length should be (0)
  }

  "A vec" should "be seq" in {
    val vec = new Vec(2, 1, 0)
    vec(0) should be (2)
    vec(1) should be (1)
    vec(2) should be (0)
  }
1
  "A vec" should "be added to a vec" in {
    val vec1 = new Vec(2, 1, 0)
    val vec2 = new Vec(0, 1, 2)
    vec1 + vec2 should be (new Vec(2, 2, 2))
  }

  "A vec" should "be multiplied by a vec" in {
    val vec1 = new Vec(2, 1, 0)
    val vec2 = new Vec(0, 1, 2)
    vec1 * vec2 should be (new Vec(0, 1, 0))
  }

  "A vec" should "be arithmetic" in {
    val ar1: Arithmetic[Vec] = new Vec(0, 1, 2)
    val ar2: Arithmetic[Vec] = new Vec(2, 1, 0)
    ar1 + ar2 should be (new Vec(2, 2, 2))
  }

}
