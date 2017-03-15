package commitlog.filesystem

import java.io.File

import com.bwsw.commitlog.filesystem.CommitLogCatalogueAllDates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CommitLogCatalogueAllDatesTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  it should "list all files in directory" in {
    val firstDir = "target/clct/1990/05/08"
    val secondDir = "target/clct/1990/05/09"
    val thirdDir = "target/clct/1990/05/10"
    val forthDir = "target/clct/1991/05/08"

    val dirs = Seq(
      new File(firstDir),
      new File(secondDir),
      new File(thirdDir),
      new File(forthDir)
    )

    dirs foreach (_.mkdirs())

    val files = Seq(
      new File(s"$firstDir/0.dat").createNewFile(),
      new File(s"$firstDir/1.dat").createNewFile(),
      new File(s"$secondDir/0.dat").createNewFile(),
      new File(s"$thirdDir/0.dat").createNewFile(),
      new File(s"$forthDir/0.dat").createNewFile()
    )

    val allFiles = new CommitLogCatalogueAllDates("target/clct")
    allFiles.catalogues.flatMap(_.listAllFiles()).length shouldBe files.length

    dirs foreach (_.delete())
  }

}
