import org.scalameter.{Context, CurveData, Persistor, Reporter, _}
import org.scalameter.utils.Tree

case class MyReporter[T]() extends Reporter[T] {

  def report(result: CurveData[T], persistor: Persistor) {
    // output context
    log(s"::Benchmark ${result.context.scope}::")
    val machineKeys = result.context.properties
      .filterKeys(Context.machine.properties.keySet.contains).toSeq.sortBy(_._1)
    for ((key, value) <- machineKeys) {
      log(s"$key: $value")
    }

    // output measurements
    for (measurement <- result.measurements) {
      log(s"${measurement.params.axisData.values.head}: ${measurement.value}")
    }

    // add a new line
    log("")
  }


  def report(result: Tree[CurveData[T]], persistor: Persistor) = true
}
