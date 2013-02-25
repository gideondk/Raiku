package nl.gideondk.raiku.mapreduce

import scala.concurrent.duration.{ DurationInt, FiniteDuration }

import shapeless.HList
import shapeless.LUBConstraint._

object MR {
  def items(bucketAndKeys: Set[(String, String)]) = new ItemMapReduceInput {
    val objs = bucketAndKeys
  }

  def bucket(name: String) = new BucketMapReduceInput {
    val bucket: String = name

    val keyFilters = None
  }
}
case class MapReduceJob[T <: HList: <<:[MapReducePhase]#λ](input: MapReduceInput, phases: T, timeout: FiniteDuration = 60 seconds)

