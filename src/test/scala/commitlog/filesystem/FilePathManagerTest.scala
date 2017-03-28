package commitlog.filesystem

import java.io.File

import com.bwsw.commitlog.filesystem.FilePathManager
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 27.01.17.
  */
class FilePathManagerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val sep = File.separator

  it should "return proper paths" in {
    val fpm = new FilePathManager("target")

    FilePathManager.CATALOGUE_GENERATOR = () => s"1111${sep}22${sep}33"
    fpm.getNextPath shouldBe s"target${sep}1111${sep}22${sep}33${sep}0"
    fpm.getNextPath shouldBe s"target${sep}1111${sep}22${sep}33${sep}1"
    fpm.getNextPath shouldBe s"target${sep}1111${sep}22${sep}33${sep}2"

    FilePathManager.CATALOGUE_GENERATOR = () => s"1111${sep}22${sep}34"
    fpm.getNextPath shouldBe s"target${sep}1111${sep}22${sep}34${sep}0"
    fpm.getNextPath shouldBe s"target${sep}1111${sep}22${sep}34${sep}1"

    FilePathManager.CATALOGUE_GENERATOR = () => s"2222${sep}22${sep}34"
    fpm.getNextPath shouldBe s"target${sep}2222${sep}22${sep}34${sep}0"
    fpm.getNextPath shouldBe s"target${sep}2222${sep}22${sep}34${sep}1"

    new File(s"target${sep}2222${sep}22${sep}35").mkdirs()
    new File(s"target${sep}2222${sep}22${sep}35${sep}0.dat").createNewFile()
    new File(s"target${sep}2222${sep}22${sep}35${sep}1.dat").createNewFile()
    FilePathManager.CATALOGUE_GENERATOR = () => s"2222${sep}22${sep}35"
    fpm.getNextPath shouldBe s"target${sep}2222${sep}22${sep}35${sep}2"
  }

  it should "throw an exception if path is not a dir" in {

    val dir = s"target${sep}fpm"
    new File(dir).mkdirs()
    new File(s"target${sep}fpm${sep}0.dat").createNewFile()
    intercept[IllegalArgumentException] {
      val fpm1 = new FilePathManager(s"target${sep}fpm${sep}0.dat")
    }
    intercept[IllegalArgumentException] {
      val fpm2 = new FilePathManager("")
    }

    FileUtils.deleteDirectory(new File(dir))
  }

  override def afterAll = {
    List(s"target${sep}1111", s"target${sep}2222").foreach(dir => FileUtils.deleteDirectory(new File(dir)))
    FilePathManager.resetCatalogueGenerator()
  }
}
