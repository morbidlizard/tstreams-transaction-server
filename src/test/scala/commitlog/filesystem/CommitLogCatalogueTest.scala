//package commitlog.filesystem
//
//import java.io.File
//
//import com.bwsw.commitlog.filesystem.CommitLogCatalogue
//import org.apache.commons.io.FileUtils
//import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
//
//class CommitLogCatalogueTest extends FlatSpec with Matchers with BeforeAndAfterEach {
//  private val sep = File.separator
//
//  override def beforeEach(): Unit = {
//    FileUtils.deleteDirectory(new File(s"target${sep}clct"))
//  }
//  override def afterEach(): Unit = beforeEach()
//
//  it should "list all files in directory" in {
//    val firstDir = s"target${sep}clct${sep}1990${sep}05${sep}08"
//    val secondDir = s"target${sep}clct${sep}1990${sep}05${sep}09"
//    val thirdDir = s"target${sep}clct${sep}1990${sep}05${sep}10"
//    val forthDir = s"target${sep}clct${sep}1991${sep}05${sep}08"
//
//    val dirs = Seq(
//      new File(firstDir),
//      new File(secondDir),
//      new File(thirdDir),
//      new File(forthDir)
//    )
//
//    dirs foreach (_.mkdirs())
//
//    val files = Seq(
//      new File(s"$firstDir${sep}0.dat").createNewFile(),
//      new File(s"$firstDir${sep}1.dat").createNewFile(),
//      new File(s"$secondDir${sep}0.dat").createNewFile(),
//      new File(s"$thirdDir${sep}0.dat").createNewFile(),
//      new File(s"$forthDir${sep}0.dat").createNewFile()
//    )
//
//    val allFiles = new CommitLogCatalogue(s"target${sep}clct")
//    allFiles.catalogues.flatMap(_.listAllFiles()).length shouldBe files.length
//
//    dirs foreach (_.delete())
//  }
//
//}
