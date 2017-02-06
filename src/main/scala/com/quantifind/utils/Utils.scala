package com.quantifind.utils

import kafka.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * Basic utils, e.g. retry block
  *
  * @author xorlev
  */

object Utils extends Logging {
  // Returning T, throwing the exception on failure
  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): T = {
    Try {
      fn
    } match {
      case Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }


  def retryTask[T](fn: => T) {
    try {
      retry(3) {
        fn
      }
    } catch {
      case e: Throwable =>
        error("Failed to run scheduled task", e)
    }
  }
}
