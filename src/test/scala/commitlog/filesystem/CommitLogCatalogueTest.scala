package commitlog.filesystem

import java.io.{File, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.Date

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by zhdanovks on 31.01.17.
  */
class CommitLogCatalogueTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val dir = "target/clct"

  override def beforeAll() = {
    new File(dir).mkdirs()
  }

  it should "delete file properly" in {
    new File("target/clct/1990/05/05").mkdirs()
    new File("target/clct/1990/05/05/0.dat").createNewFile()
    new File("target/clct/1990/05/05/0.md5").createNewFile()
    new File("target/clct/1990/05/05/1.dat").createNewFile()
    new File("target/clct/1990/05/05/2.md5").createNewFile()

    val commitLogCatalogue = new CommitLogCatalogue("target/clct", new Date(641836800000L))
    commitLogCatalogue.deleteFile("0.dat") shouldBe true
    new File("target/clct/1990/05/05/0.dat").exists() shouldBe false
    new File("target/clct/1990/05/05/0.md5").exists() shouldBe false
    commitLogCatalogue.deleteFile("1.dat") shouldBe false
    new File("target/clct/1990/05/05/1.dat").exists() shouldBe false
    commitLogCatalogue.deleteFile("2.dat") shouldBe false
    new File("target/clct/1990/05/05/2.md5").exists() shouldBe true
    commitLogCatalogue.deleteFile("3.dat") shouldBe false
  }

  it should "delete files for date properly" in {
    val commitLog = new CommitLog(1, dir)
    new File("target/clct/1990/05/06").mkdirs()
    new File("target/clct/1990/05/06/0.dat").createNewFile()
    new File("target/clct/1990/05/06/0.md5").createNewFile()
    new File("target/clct/1990/05/06/1.dat").createNewFile()
    new File("target/clct/1990/05/06/1.md5").createNewFile()
    new File("target/clct/1990/05/06/2.dat").createNewFile()
    new File("target/clct/1990/05/06/3.md5").createNewFile()
    val commitLogCatalogue = new CommitLogCatalogue("target/clct", new Date(641923200000L))
    commitLogCatalogue.deleteAllFiles() shouldBe true
    new File("target/clct/1990/05/06/0.dat").exists() shouldBe false
    new File("target/clct/1990/05/06/0.md5").exists() shouldBe false
    new File("target/clct/1990/05/06/1.dat").exists() shouldBe false
    new File("target/clct/1990/05/06/1.md5").exists() shouldBe false
    new File("target/clct/1990/05/06/2.dat").exists() shouldBe false
    new File("target/clct/1990/05/06/3.md5").exists() shouldBe false
    new File("target/clct/1990/05/07").mkdirs()
    val commitLogCatalogue1 = new CommitLogCatalogue("target/clct", new Date(642009600000L))
    commitLogCatalogue1.deleteAllFiles() shouldBe true
  }

  it should "list all files in directory" in {
    new File("target/clct/1990/05/08").mkdirs()
    new File("target/clct/1990/05/08/0.dat").createNewFile()
    new File("target/clct/1990/05/08/0.md5").createNewFile()
    new File("target/clct/1990/05/08/1.dat").createNewFile()
    new File("target/clct/1990/05/08/1.md5").createNewFile()
    val commitLogCatalogue = new CommitLogCatalogue("target/clct", new Date(642096000000L))
    val received = commitLogCatalogue.listAllFiles()
    received.size == 2 shouldBe true
    for (file <- received) {
      file.getFile().toString == "target/clct/1990/05/08/0.dat" ||
        file.getFile().toString == "target/clct/1990/05/08/1.dat" shouldBe true
    }
  }

  override def afterAll = {
    List(dir).foreach(dir =>
      Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }))
  }
}
