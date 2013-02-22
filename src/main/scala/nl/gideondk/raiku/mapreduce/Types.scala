package nl.gideondk.raiku.mapreduce

import scalaz._

import scala.concurrent.duration._

case class MapReduceJob(input: MapReduceInput, mrPipe: MapReducePipe, timeout: FiniteDuration = 60 seconds) {
  def wait(duration: FiniteDuration) = this.copy(timeout = duration)
}

trait MapReduceInput {
  def |>>(pipe: MapReducePipe) = MapReduceJob(this, pipe)
}

trait BucketMapReduceInput extends MapReduceInput {
  def bucket: String

  def keyFilters: Option[Set[Set[String]]]
}

trait ObjectBasedMapReducedInput extends MapReduceInput

trait ItemMapReduceInput extends ObjectBasedMapReducedInput {
  def objs: Set[(String, String)]
}

trait AnnotatedMapReduceInput extends ObjectBasedMapReducedInput {
  def objs: Set[(String, String, String)]
}

trait IdxMapReduceInput extends MapReduceInput {
  def bucket: String
}

trait BinIdxMapReduceInput extends IdxMapReduceInput {
  def indexKey: String

  def indexValue: String
}

trait IntIdxMapReduceInput extends IdxMapReduceInput {
  def indexKey: String

  def indexValue: Either[Int, Range]
}

object MR {
  def items(bucketAndKeys: Set[(String, String)]) = new ItemMapReduceInput {
    val objs = bucketAndKeys
  }

  def bucket(name: String) = new BucketMapReduceInput {
    val bucket: String = name

    val keyFilters = None
  }
}

case class MapReducePipe(phases: NonEmptyList[MapReducePhase]) {
  def |*(m: MapPhase) = MapReducePipe(phases :::> List(m))

  def |-(r: ReducePhase) = MapReducePipe(phases :::> List(r))
}

trait MapReducePhase {
  def fn: MapReduceFunction

  def arg: Option[String]

  def keep: Boolean

  override def toString = "("+fn.value+", "+arg+", "+keep+")"
}

trait MapPhase extends MapReducePhase

trait ReducePhase extends MapReducePhase

object MapPhase {
  def apply(f: MapReduceFunction, k: Boolean = false, a: Option[String] = None) = {
    new MapPhase {
      override val fn = f
      override val keep = k
      override val arg = a
    }
  }
}

object ReducePhase {
  def apply(f: MapReduceFunction, k: Boolean = false, a: Option[String] = None) = {
    new ReducePhase {
      override val fn = f
      override val keep = k
      override val arg = a
    }
  }
}

trait MapReduceFunction {
  def value: String
}

case class MRBuiltinFunction(value: String) extends MapReduceFunction

case class MRFunction(value: String) extends MapReduceFunction
