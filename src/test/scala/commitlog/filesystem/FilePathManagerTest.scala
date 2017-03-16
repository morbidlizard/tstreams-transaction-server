package commitlog.filesystem

import java.io.File

import com.bwsw.commitlog.filesystem.FilePathManager
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 27.01.17.
  */
class FilePathManagerTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  it should "return proper paths" in {
    val fpm = new FilePathManager("target")

    FilePathManager.CATALOGUE_GENERATOR = () => "1111/22/33"
    fpm.getNextPath() shouldBe "target/1111/22/33/0"
    fpm.getNextPath() shouldBe "target/1111/22/33/1"
    fpm.getNextPath() shouldBe "target/1111/22/33/2"

    FilePathManager.CATALOGUE_GENERATOR = () => "1111/22/34"
    fpm.getNextPath() shouldBe "target/1111/22/34/0"
    fpm.getNextPath() shouldBe "target/1111/22/34/1"

    FilePathManager.CATALOGUE_GENERATOR = () => "2222/22/34"
    fpm.getNextPath() shouldBe "target/2222/22/34/0"
    fpm.getNextPath() shouldBe "target/2222/22/34/1"

    new File("target/2222/22/35").mkdirs()
    new File("target/2222/22/35/0.dat").createNewFile()
    new File("target/2222/22/35/1.dat").createNewFile()
    FilePathManager.CATALOGUE_GENERATOR = () => "2222/22/35"
    fpm.getNextPath() shouldBe "target/2222/22/35/2"
  }

  it should "throw an exception if path is not a dir" in {
    val dir = "target/fpm"
    new File(dir).mkdirs()
    new File("target/fpm/0.dat").createNewFile()
    intercept[IllegalArgumentException] {
      val fpm1 = new FilePathManager("target/fpm/0.dat")
    }
    intercept[IllegalArgumentException] {
      val fpm2 = new FilePathManager("")
    }

    FileUtils.deleteDirectory(new File(dir))
  }

  override def afterAll = {
    List("target/1111", "target/2222").foreach(dir => FileUtils.deleteDirectory(new File(dir)))
    FilePathManager.resetCatalogueGenerator()
  }
}
