package com.gilt.spark.manager

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration

object utils {
  implicit class AwaitableFuture[A](val f: Future[A]) extends AnyVal {
    @inline def await: A = Await.result(f, Duration.Inf)
  }

}
