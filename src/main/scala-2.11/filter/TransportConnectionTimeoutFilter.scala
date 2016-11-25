package filter

import com.twitter.finagle.context.Deadline
import com.twitter.finagle.{Service, ServiceTimeoutException, SimpleFilter}
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.{Backoff, RetryExceptionsFilter, RetryPolicy}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future, Throw, Try}
import com.twitter.util.TimeConversions._

object TransportConnectionTimeoutFilter {
  def retryFilterConnection[Req, Rep](timeoutConnection: Int, timeoutExponentialBetweenRetries: Int, logger: Logger, message: String) = {
    val retryConditionToConnect: PartialFunction[Try[Nothing], Boolean] = {
      case Throw(error) => error match {
        case e: ServiceTimeoutException =>
          logger.log(Level.INFO, message)
          true
        case e: com.twitter.finagle.ChannelWriteException =>
          logger.log(Level.INFO, message)
          true
        case e =>
          Logger.get().log(Level.ERROR, e.getMessage)
          false
      }
      case _ => false
    }

    val retryPolicyConnection = RetryPolicy.backoff(
      Backoff.exponentialJittered
      (timeoutExponentialBetweenRetries.milliseconds, timeoutConnection.milliseconds)
    )(retryConditionToConnect)

    new RetryExceptionsFilter[Req, Rep](retryPolicyConnection, HighResTimer.Default)
  }
}
