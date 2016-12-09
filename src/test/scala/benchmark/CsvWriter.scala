package benchmark

import java.io.{PrintWriter, File}

trait CsvWriter {
  def writeTransactionsAndTime(filename: String, data: IndexedSeq[(Int, Long, Long)]) = {
    val file = new File(filename)
    val bw = new PrintWriter(file)
    bw.write("Number of transaction, Open time (ms), Close time (ms)\n")
    data.foreach(x => bw.write(x._1 + ", " + x._2 + ", " + x._3 + "\n"))
    bw.close()
  }

  def writeDataTransactionsAndTime(filename: String, data: IndexedSeq[(Int, Long)]) = {
    val file = new File(filename)
    val bw = new PrintWriter(file)
    bw.write("Number of transaction, Time (ms)\n")
    data.foreach(x => bw.write(x._1 + ", " + x._2 + "\n"))
    bw.close()
  }

  def writeTransactionsLifeCycleAndTime(filename: String, data: IndexedSeq[(Int, Long)]) = {
    val file = new File(filename)
    val bw = new PrintWriter(file)
    bw.write("Number of transaction life cycle, Time (ms)\n")
    data.foreach(x => bw.write(x._1 + ", " + x._2 + "\n"))
    bw.close()
  }
}
