package com.bwsw.tstreamstransactionserver.netty.client


import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import com.bwsw.tstreamstransactionserver.exception.Throwable.RequestTimeoutException

import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}
import scala.concurrent.duration.Duration
import io.netty.util.{HashedWheelTimer, Timeout}


object TimeoutScheduler{
  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
  def scheduleTimeout(promise:ScalaPromise[_], after:Duration): Timeout = {
    timer.newTimeout((timeout: Timeout) => {
      promise.tryFailure(new RequestTimeoutException(1, after.toMillis))
    }, after.toNanos, TimeUnit.NANOSECONDS)
  }

  def withTimeout[T](fut:ScalaFuture[T])(implicit ec:ExecutionContext, after:Duration): ScalaFuture[T] = {
    val prom = ScalaPromise[T]()
    val timeout = TimeoutScheduler.scheduleTimeout(prom, after)
    val combinedFut = ScalaFuture.firstCompletedOf(collection.immutable.Seq(fut, prom.future))
    fut onComplete (_ => timeout.cancel())
    combinedFut
  }
}
