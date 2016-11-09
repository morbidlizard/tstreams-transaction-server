import com.twitter.io.TempDirectory
import com.twitter.logging.{Level, Logger}
import org.scalameter.Bench
import org.scalameter.api._
import org.scalameter.reporting.LoggingReporter
import org.scalameter.picklers.Implicits._
import transactionService.impl.db.SleepyCat
import transactionService.impl.db.SleepyCat.{MyKey, MyTransaction}

object BerkeleyDBCursorBenchmark extends Bench[Double] {
  lazy val executor = LocalExecutor(
    Executor.Warmer.Zero,
    Aggregator.min[Double],
    measurer)

  lazy val measurer = new Measurer.Default
  lazy val reporter = new MyReporter[Double]()
  lazy val persistor = Persistor.None

  val interval = 1000000
  val minValue = interval
  val maxValue = 10000000

  val sizes = Gen.range("size")(minValue, maxValue, interval)

  val factor = 2
  val factors = Gen.exponential("factor")(minValue, maxValue, factor)


  val ranges1 = for {
    size <- sizes
  } yield (size - interval, size)

  val ranges2 = for {
    size <- factors
  } yield (size/factor, size)



  val (env, store) = SleepyCat.setup()

  (minValue to maxValue).foreach{_=>
    val txn = new MyTransaction(scala.util.Random.nextLong(), 0, scala.util.Random.nextInt(), java.time.Clock.systemUTC().millis(), java.time.Clock.systemUTC().millis(), -1, 0)
    SleepyCat.saveTransaction(txn, store)
  }


  measure method "getTransactions" in {
    using(ranges1) config (
      exec.benchRuns -> 1
      ) in { r =>
      val from = r._1
      val to   = r._2
      Logger.get().log(Level.INFO,s"${r._1} to ${r._2}")

      SleepyCat.getTransactionRange(new MyKey(r._1,0,0L), new MyKey(r._2,Int.MaxValue,Long.MaxValue), store)
    }
  }
}

