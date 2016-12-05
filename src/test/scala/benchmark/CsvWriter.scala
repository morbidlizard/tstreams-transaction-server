package benchmark

import java.io.{PrintWriter, File}

trait CsvWriter {
  def writeRecordsAndTime(filename: String, data: IndexedSeq[(Int, Long)], dataSize: Int) = {
    val file = new File(filename)
    val bw = new PrintWriter(file)
    bw.write("Number of records, Time (ms)\n")
    data.foreach(x => bw.write(x._1 * dataSize + ", " + x._2 + "\n"))
    bw.close()
  }

  def writeTransactionsAndTime(filename: String, data: IndexedSeq[(Int, Long)]) = {
    val file = new File(filename)
    val bw = new PrintWriter(file)
    bw.write("Number of transactions, Time (ms)\n")
    data.foreach(x => bw.write(x._1 + ", " + x._2 + "\n"))
    bw.close()
  }
}
