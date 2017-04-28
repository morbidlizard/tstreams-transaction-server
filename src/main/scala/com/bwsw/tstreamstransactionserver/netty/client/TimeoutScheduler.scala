package com.bwsw.tstreamstransactionserver.netty.client


import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import com.bwsw.tstreamstransactionserver.exception.Throwable.RequestTimeoutException

import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}
import scala.concurrent.duration.Duration
import io.netty.util.{HashedWheelTimer, Timeout}
import org.slf4j.LoggerFactory


object TimeoutScheduler{
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
  def scheduleTimeout(promise:ScalaPromise[_], after:Duration, reqId: Long): Timeout = {
    timer.newTimeout((timeout: Timeout) => {
      val requestTimeoutException = new RequestTimeoutException(reqId, after.toMillis)
      val isExpired = promise.tryFailure(requestTimeoutException)
      if (isExpired && logger.isDebugEnabled)
        logger.debug(requestTimeoutException.getMessage)
      if (isExpired) println("asdasdasdasdsd")
    }, after.toNanos, TimeUnit.NANOSECONDS)
  }

  def withTimeout[T](fut:ScalaFuture[T])(implicit ec:ExecutionContext, after:Duration, reqId: Long): ScalaFuture[T] = {
    val prom = ScalaPromise[T]()
    val timeout = TimeoutScheduler.scheduleTimeout(prom, after, reqId)
    val combinedFut = ScalaFuture.firstCompletedOf(collection.immutable.Seq(fut, prom.future))
    fut onComplete (_ => timeout.cancel())
    combinedFut
  }
}
