package nl.gideondk.raiku.mapreduce

import scala.concurrent.duration.{ DurationInt, FiniteDuration }

import shapeless.HList
import shapeless.LUBConstraint._

object MR {
  def items(bucketAndKeys: Set[(String, String)]) = new ItemMapReduceInput {
    val objs = bucketAndKeys
  }

  def annotatedItems(bucketKeysAndAnnotation: Set[(String, String, String)]) = new AnnotatedMapReduceInput {
    val objs = bucketKeysAndAnnotation
  }

  def binIdx(bucketName: String, idx: String, idxv: String) = new BinIdxMapReduceInput {
    val indexKey = idx

    val bucket = bucketName

    val indexValue = idxv
  }

  def intIdx(bucketName: String, idx: String, idxv: Int) = new IntIdxMapReduceInput {
    val indexKey = idx

    val bucket = bucketName

    val indexValue = Left(idxv)
  }

  def rangeIdx(bucketName: String, idx: String, idxv: Range) = new IntIdxMapReduceInput {
    val indexKey = idx

    val bucket = bucketName

    val indexValue = Right(idxv)
  }

  def bucket(name: String) = new BucketMapReduceInput {
    val bucket: String = name

    val keyFilters = None
  }

  def bucket(name: String, filters: Set[Set[String]]) = new BucketMapReduceInput {
    val bucket: String = name

    val keyFilters = Some(filters)
  }

}

case class MapReduceJob[T <: HList: <<:[MapReducePhase]#λ](input: MapReduceInput, phases: T, timeout: FiniteDuration = 60 seconds)

