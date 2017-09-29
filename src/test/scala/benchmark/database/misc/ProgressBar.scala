package benchmark.database.misc

class ProgressBar(val total:Int = 100, private var value: Int = 0, val size: Int = 20) {
  private def progress = value * size / total

  draw(true)

  def set(value: Int): Unit = {
    this.value = value

    this.draw(false)
  }

  def draw(start: Boolean): Unit = {
    if (!start) print("\r")
    print(
      "["
        + "#" * progress
        + " " * (size - progress)
        + "]"
    )
  }

  def finish(): Unit = {
    print("\r")
  }
}
