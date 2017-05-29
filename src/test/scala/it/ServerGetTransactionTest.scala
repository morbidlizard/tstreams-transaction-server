package it

import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.scalatest.{FlatSpec, Matchers}
import util.Utils

import scala.concurrent.Await
import scala.concurrent.duration._

class ServerGetTransactionTest
  extends FlatSpec
    with Matchers
{
  private val secondsToWait = 5.seconds

  "Client" should "get transaction ID that not less that current time" in {
    val bundle = Utils.startTransactionServerAndClient(
      new ServerBuilder(), new ClientBuilder()
    )
    val client = bundle.client

    val currentTime = System.currentTimeMillis()
    val result = Await.result(client.getTransaction(), secondsToWait)

    result shouldBe >= (currentTime)

    bundle.close()
  }

  it should "get transaction ID by timestamp" in {
    val bundle = Utils.startTransactionServerAndClient(
      new ServerBuilder(), new ClientBuilder()
    )
    val client = bundle.client

    val currentTime = System.currentTimeMillis()
    val result = Await.result(client.getTransaction(currentTime), secondsToWait)

    result shouldBe (currentTime*100000)

    bundle.close()
  }

}
