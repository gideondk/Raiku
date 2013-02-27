package nl.gideondk

import raiku.commands._
import raiku.serialization._
import raiku.monads.{ ValidatedFuture, ValidatedFutureIO }
import scalaz._
import Scalaz._

import scala.concurrent.ExecutionContext.Implicits.global
import scalaz.Success
import scalaz.Failure
import scala.Some

package object raiku {
  implicit def stringToVClock(s: String): VClock = VClock(s.getBytes)

  implicit def intToRArgument(i: Int): RArgument = RArgument(Option(i))
  implicit def intToRWArgument(i: Int): RWArgument = RWArgument(Option(i))
  implicit def intToWArgument(i: Int): WArgument = WArgument(Option(i))

  implicit def intToPRArgument(i: Int): PRArgument = PRArgument(Option(i))
  implicit def intToPWArgument(i: Int): PWArgument = PWArgument(Option(i))
  implicit def intToDWArgument(i: Int): DWArgument = DWArgument(Option(i))

  implicit def booleanToBasicQuoromArgument(b: Boolean): BasicQuorumArgument = BasicQuorumArgument(Option(b))
  implicit def booleanToNotFoundArgument(b: Boolean): NotFoundOkArgument = NotFoundOkArgument(Option(b))
  implicit def booleanToIfNotModifiedArgument(b: Boolean): IfNotModifiedArgument = IfNotModifiedArgument(Option(b))
  implicit def booleanToIfNonMatchedArgument(b: Boolean): IfNonMatchedArgument = IfNonMatchedArgument(Option(b))

  implicit def vclockToIfModifiedArgument(v: VClock): IfModifiedArgument = IfModifiedArgument(Option(v))
  implicit def booleanToOnlyHeadArgument(b: Boolean): OnlyHeadArgument = OnlyHeadArgument(Option(b))

  implicit def booleanToReturnHeadArgument(b: Boolean): ReturnHeadArgument = ReturnHeadArgument(Option(b))
  implicit def booleanToReturnBodyArgument(b: Boolean): ReturnBodyArgument = ReturnBodyArgument(Option(b))
  implicit def booleanToDeletedVClockArgument(b: Boolean): DeletedVClockArgument = DeletedVClockArgument(Option(b))

  implicit def vclockToVClockArgument(v: VClock): VClockArgument = VClockArgument(Option(v))

  implicit def ValidatedFutureIORWListToValidatedFutureIOOptT[T](v: ValidatedFutureIO[List[RWObject]])(implicit converter: RaikuConverter[T]): ValidatedFutureIO[Option[T]] = {
    ValidatedFutureIO(v.run.map {
      x ⇒
        ValidatedFuture(x.run.map { v ⇒
          v match {
            case Success(List(obj)) ⇒ converter.read(obj).map(_.some)
            case Success(List())    ⇒ none.success[Throwable]
            case Success(List(_*))  ⇒ new Exception("There were siblings").failure[Option[T]]
            case Failure(fail)      ⇒ fail.failure[Option[T]]
          }
        })
    })
  }

  implicit def ValidatedFutureIORWListToValidatedFutureIOOptRW(v: ValidatedFutureIO[List[RWObject]]): ValidatedFutureIO[Option[RWObject]] = {
    ValidatedFutureIO(v.run.map {
      x ⇒
        ValidatedFuture(x.run.map { v ⇒
          v match {
            case Success(List(obj)) ⇒ Some(obj).success
            case Success(List())    ⇒ none.success[Throwable]
            case Success(List(_*))  ⇒ new Exception("There were siblings").failure[Option[RWObject]]
            case Failure(fail)      ⇒ fail.failure[Option[RWObject]]
          }
        })
    })
  }
}