package authService

import authService.rpc.AuthService
import com.twitter.finagle.Thrift
import com.twitter.logging.Logger
import com.twitter.util.{Throw, Try, Future => TwitterFuture}
import exception.Throwables.tokenInvalidException
import filter.Filter

class AuthClient(ipAddress: String, authTimeoutConnection: Int, authTimeoutExponentialBetweenRetries: Int) extends AuthService[TwitterFuture] {
  private val logger = Logger.get(this.getClass)
  private val client = Thrift.client
    .withSessionQualifier.noFailFast
    .withSessionQualifier.noFailureAccrual

  def timeOutFilter[Req, Rep] = Filter
    .filter[Req, Rep](authTimeoutConnection, authTimeoutExponentialBetweenRetries, Filter.retryConditionToConnect)


  private def interface = {
    val interface= client.newServiceIface[AuthService.ServiceIface](ipAddress, "transaction")
    interface.copy(
      authenticate = timeOutFilter andThen interface.authenticate,
      isValid = timeOutFilter andThen interface.isValid
    )
  }

  private final val request = Thrift.client.newMethodIface(interface)
  override def authenticate(login: String, password: String): TwitterFuture[String] =  request.authenticate(login,password)
  override def isValid(token: String): TwitterFuture[Boolean] = request.isValid(token) flatMap (valid =>
    if (valid) TwitterFuture.value(valid) else TwitterFuture.exception(tokenInvalidException)
    )
}
