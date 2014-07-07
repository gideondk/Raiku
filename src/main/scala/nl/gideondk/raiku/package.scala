package nl.gideondk

import raiku.commands._
import scala.concurrent.duration.Duration

package object raiku {
  implicit def stringToVClock(s: String): VClock = VClock(s.getBytes)

  implicit def intToRArgument(i: Int): RArgument = RArgument(Option(i))
  implicit def intToRWArgument(i: Int): RWArgument = RWArgument(Option(i))
  implicit def intToWArgument(i: Int): WArgument = WArgument(Option(i))

  implicit def intToPRArgument(i: Int): PRArgument = PRArgument(Option(i))
  implicit def intToPWArgument(i: Int): PWArgument = PWArgument(Option(i))
  implicit def intToDWArgument(i: Int): DWArgument = DWArgument(Option(i))

  implicit def durationToTimeoutArgument(d: Duration): TimeoutArgument = TimeoutArgument(Option(d))

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

  implicit def unwrapRaikuValue[T](rv: RaikuValue[T]): T = rv.value.getOrElse(throw new Exception("Only object Meta is available."))

  implicit class StringRange(val b: String) extends AnyVal {
    def to(e: String) = RaikuStringRange(b, e)
  }
}