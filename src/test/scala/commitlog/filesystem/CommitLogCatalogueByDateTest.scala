//package commitlog.filesystem
//
//import java.io.{File, IOException}
//import java.nio.file._
//import java.nio.file.attribute.BasicFileAttributes
//import java.util.Date
//
//import com.bwsw.commitlog.CommitLog
//import com.bwsw.commitlog.filesystem.CommitLogCatalogueByDate
//import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
//
///**
//  * Created by zhdanovks on 31.01.17.
//  */
//class CommitLogCatalogueByDateTest extends FlatSpec with Matchers with BeforeAndAfterAll {
//  val sep = File.separatorChar
//  val dir = new StringBuffer().append("target").append(sep).append("clct").toString
//
//  override def beforeAll() = {
//    new File(dir).mkdirs()
//  }
//
//  it should "delete file properly" in {
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05").mkdirs()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05${sep}0.dat").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05${sep}0.md5").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05${sep}1.dat").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05${sep}2.md5").createNewFile()
//
//    val commitLogCatalogue = new CommitLogCatalogueByDate(dir, new Date(641836800000L))
//    commitLogCatalogue.deleteFile("0.dat") shouldBe true
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05${sep}0.dat").exists() shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05${sep}0.md5").exists() shouldBe false
//    commitLogCatalogue.deleteFile("1.dat") shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05${sep}1.dat").exists() shouldBe false
//    commitLogCatalogue.deleteFile("2.dat") shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}05${sep}2.md5").exists() shouldBe true
//    commitLogCatalogue.deleteFile("3.dat") shouldBe false
//  }
//
//  it should "delete files for date properly" in {
//    val commitLog = new CommitLog(1, dir)
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06").mkdirs()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}0.dat").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}0.md5").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}1.dat").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}1.md5").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}2.dat").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}3.md5").createNewFile()
//    val commitLogCatalogue = new CommitLogCatalogueByDate(dir, new Date(641923200000L))
//    commitLogCatalogue.deleteAllFiles() shouldBe true
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}0.dat").exists() shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}0.md5").exists() shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}1.dat").exists() shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}1.md5").exists() shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}2.dat").exists() shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}06${sep}3.md5").exists() shouldBe false
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}07").mkdirs()
//    val commitLogCatalogue1 = new CommitLogCatalogueByDate(dir, new Date(642009600000L))
//    commitLogCatalogue1.deleteAllFiles() shouldBe true
//  }
//
//  it should "list all files in directory" in {
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}08").mkdirs()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}08${sep}0.dat").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}08${sep}0.md5").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}08${sep}1.dat").createNewFile()
//    new File(s"target${sep}clct${sep}1990${sep}05${sep}08${sep}1.md5").createNewFile()
//    val commitLogCatalogue = new CommitLogCatalogueByDate(dir, new Date(642096000000L))
//    val received = commitLogCatalogue.listAllFiles()
//    received.size == 2 shouldBe true
//    for (file <- received) {
//      file.getFile.toString == s"target${sep}clct${sep}1990${sep}05${sep}08${sep}0.dat" ||
//        file.getFile.toString == s"target${sep}clct${sep}1990${sep}05${sep}08${sep}1.dat" shouldBe true
//    }
//  }
//
//  override def afterAll = {
//    List(dir).foreach(dir =>
//      Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor[Path]() {
//        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
//          Files.delete(file)
//          FileVisitResult.CONTINUE
//        }
//
//        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
//          Files.delete(dir)
//          FileVisitResult.CONTINUE
//        }
//      }))
//  }
//}
