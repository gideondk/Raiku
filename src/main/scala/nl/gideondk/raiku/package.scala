package nl.gideondk

import scalaz._
import Scalaz._

import scala.concurrent.ExecutionContext.Implicits.global

package object raiku {
  implicit def intToIntOpt(a: Int): Option[Int] = Some(a)

  implicit def ValidatedFutureIORWListToValidatedFutureIOOptT[T](v: ValidatedFutureIO[List[RaikuRWObject]])(implicit converter: RaikuConverter[T]): ValidatedFutureIO[Option[T]] = {
    ValidatedFutureIO(v.io.map {
      x =>
        ValidatedFuture(x.future.map { v =>
          v match {
            case Success(List(obj)) => converter.read(obj).map(_.some)
            case Success(List()) => none.success[Throwable]
            case Success(List(_*)) => new Exception("There were siblings").failure[Option[T]]
            case Failure(fail) => fail.failure[Option[T]]
          }
        })
    })
  }

  implicit def ValidatedFutureIORWListToValidatedFutureIOOptRW(v: ValidatedFutureIO[List[RaikuRWObject]]): ValidatedFutureIO[Option[RaikuRWObject]] = {
    ValidatedFutureIO(v.io.map {
      x =>
        ValidatedFuture(x.future.map { v =>
          v match {
            case Success(List(obj)) => Some(obj).success
            case Success(List()) => none.success[Throwable]
            case Success(List(_*)) => new Exception("There were siblings").failure[Option[RaikuRWObject]]
            case Failure(fail) => fail.failure[Option[RaikuRWObject]]
          }
        })
    })
  }
}