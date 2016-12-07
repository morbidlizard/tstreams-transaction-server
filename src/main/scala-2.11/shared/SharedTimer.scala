package shared

import com.twitter.finagle.param.HighResTimer
import com.twitter.util.Timer


object SharedTimer {
  final val highResTimer = HighResTimer.Default
}
