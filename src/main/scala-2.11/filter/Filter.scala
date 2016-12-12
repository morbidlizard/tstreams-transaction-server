package filter

import com.twitter.finagle.{Service, ServiceTimeoutException, SimpleFilter}
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.{Backoff, RetryExceptionsFilter, RetryPolicy}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Throw, Try, Future => TwitterFuture}
import com.twitter.util.TimeConversions._
import configProperties.LogMessage

object Filter {

  def filter[Req, Rep](timeoutConnection: Int, timeoutExponentialBetweenRetries: Int, condition: PartialFunction[Try[Nothing], Boolean]) = {
    val retryPolicy = RetryPolicy.backoff(
      Backoff.const(timeoutExponentialBetweenRetries.milliseconds).take(timeoutConnection/timeoutExponentialBetweenRetries)
//      (timeoutExponentialBetweenRetries.milliseconds, timeoutConnection.milliseconds)
    )(condition)

    new RetryExceptionsFilter[Req, Rep](retryPolicy, shared.SharedTimer.highResTimer)
  }

  
  val retryConditionToConnectToMaster: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(error) => error match {
      case e: ServiceTimeoutException =>
        Logger.get().log(Level.INFO, LogMessage.tryingToConnectToMasterServer)
        true
      case e: com.twitter.finagle.ChannelWriteException =>
        Logger.get().log(Level.INFO, LogMessage.tryingToConnectToMasterServer)
        true
      case e =>
        Logger.get().log(Level.ERROR, e.getMessage)
        false
    }
    case _ => false
  }


  val retryConditionToGetMaster: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(error) => error match {
      case e: java.util.NoSuchElementException =>
        Logger.get().log(Level.INFO, LogMessage.tryingToGetMasterFromZookeeper)
        true
      case e =>
        Logger.get().log(Level.ERROR, e.getMessage)
        false
    }
    case _ => false
  }
}
