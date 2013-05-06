package nl.gideondk.raiku.commands
import nl.gideondk.raiku._

trait RaikuArgument[T] {
  def v: Option[T]
}

trait RaikuIntArgument extends RaikuArgument[Int]
trait RaikuStringArgument extends RaikuArgument[String]
trait RaikuBooleanArgument extends RaikuArgument[Boolean]
trait RaikuVClockArgument extends RaikuArgument[VClock]

case class RArgument(v: Option[Int] = None) extends RaikuIntArgument
case class RWArgument(v: Option[Int] = None) extends RaikuIntArgument
case class WArgument(v: Option[Int] = None) extends RaikuIntArgument

case class PRArgument(v: Option[Int] = None) extends RaikuIntArgument
case class PWArgument(v: Option[Int] = None) extends RaikuIntArgument
case class DWArgument(v: Option[Int] = None) extends RaikuIntArgument

case class BasicQuorumArgument(v: Option[Boolean] = None) extends RaikuBooleanArgument
case class NotFoundOkArgument(v: Option[Boolean] = None) extends RaikuBooleanArgument

case class IfNotModifiedArgument(v: Option[Boolean] = None) extends RaikuBooleanArgument
case class IfNonMatchedArgument(v: Option[Boolean] = None) extends RaikuBooleanArgument

case class IfModifiedArgument(v: Option[VClock] = None) extends RaikuVClockArgument
case class OnlyHeadArgument(v: Option[Boolean] = None) extends RaikuBooleanArgument

case class ReturnHeadArgument(v: Option[Boolean] = None) extends RaikuBooleanArgument
case class ReturnBodyArgument(v: Option[Boolean] = None) extends RaikuBooleanArgument

case class DeletedVClockArgument(v: Option[Boolean] = None) extends RaikuBooleanArgument
case class VClockArgument(v: Option[VClock] = None) extends RaikuVClockArgument

object RaikuArgument {

}