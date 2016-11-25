package authService

import authService.rpc.AuthService
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.{Backoff, RetryExceptionsFilter, RetryPolicy}
import com.twitter.finagle.{ServiceTimeoutException, Thrift}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Throw,Try, Future => TwitterFuture}
import com.twitter.util.TimeConversions._

class ClientAuth(ipAddress: String, authTimeoutConnection: Int, authTimeoutExponentialBetweenRetries: Int) extends AuthService[TwitterFuture] {
  private val logger = Logger.get(this.getClass)
  private val client = Thrift.client
    .withSessionQualifier.noFailFast
    .withSessionQualifier.noFailureAccrual

  private val retryConditionToConnect: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(error) => error match {
      case e: ServiceTimeoutException =>
        logger.log(Level.INFO, resource.LogMessage.tryingToConnectToAuthServer)
        true
      case e: com.twitter.finagle.ChannelWriteException =>
        logger.log(Level.INFO, resource.LogMessage.tryingToConnectToAuthServer)
        true
      case e =>
        Logger.get().log(Level.ERROR, e.getMessage)
        false
    }
    case _ => false
  }
  private val retryPolicyConnection = RetryPolicy.backoff(
    Backoff.exponentialJittered
    (authTimeoutExponentialBetweenRetries.milliseconds, authTimeoutConnection.milliseconds)
  )(retryConditionToConnect)

  private def retryFilterConnection[Req, Rep] = new RetryExceptionsFilter[Req, Rep](retryPolicyConnection, HighResTimer.Default)

  private def interface = {
    val interface= client.newServiceIface[AuthService.ServiceIface](ipAddress, "transaction")
    interface.copy(
      authenticate = retryFilterConnection andThen interface.authenticate,
      isValid = retryFilterConnection andThen interface.isValid
    )
  }

  private final val request = Thrift.client.newMethodIface(interface)
  override def authenticate(login: String, password: String): TwitterFuture[String] =  request.authenticate(login,password)
  override def isValid(token: String): TwitterFuture[Boolean] = request.isValid(token)
}
