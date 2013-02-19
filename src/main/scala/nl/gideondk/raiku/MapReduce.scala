import scalaz._
import Scalaz._

object MR {
  implicit def mapPhaseToMapReducePipe(mp: MapPhase): MapReducePipe = MapReducePipe(NonEmptyList(mp))
  implicit def reducePhaseToMapReducePipe(rp: ReducePhase): MapReducePipe = MapReducePipe(NonEmptyList(rp))

  MapPhase("123") |* MapPhase("321")

}

case class MapReducePipe(phases: NonEmptyList[MapReducePhase]) {
  def |*(m: MapPhase) = MapReducePipe(phases :::> List(m))
  def |-(r: ReducePhase) = MapReducePipe(phases :::> List(r))
}

trait MapReducePhase {
  def fn: String
  def arg: Option[String]
  def keep: Boolean
}

trait MapPhase extends MapReducePhase
trait ReducePhase extends MapReducePhase

object MapPhase {
  def apply(f: String, k: Boolean = false, a: Option[String] = None) = {
    new MapPhase {
      override val fn = f
      override val keep = k
      override val arg = a
    }
  }
}

object ReducePhase {
  def apply(f: String, k: Boolean = false, a: Option[String] = None) = {
    new ReducePhase {
      override val fn = f
      override val keep = k
      override val arg = a
    }
  }
}