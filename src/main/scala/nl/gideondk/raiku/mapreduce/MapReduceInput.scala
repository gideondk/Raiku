package nl.gideondk.raiku.mapreduce

import shapeless.{ :: ⇒ ::, HList }
import shapeless.{ HNil, LUBConstraint, PrependAux }
import shapeless.HList.hlistOps

import MapReducePhases.keeped

trait MapReduceInput {
  import MapReducePhases._

  def |>>[T <: HList, Z <: HList](mrp: MapReducePhasesMHead[T])(implicit p: PrependAux[T, MapPhase :: HNil, Z],
                                                                bct: LUBConstraint[Z, MapReducePhase]) = {
    val phases = mrp.phases :+ keeped(mrp.head)
    MapReduceJob(this, phases)
  }

  def |>>[T <: HList, Z <: HList](mrp: MapReducePhasesRHead[T])(implicit p: PrependAux[T, ReducePhase :: HNil, Z],
                                                                bct: LUBConstraint[Z, MapReducePhase]) = {
    val phases = mrp.phases :+ keeped(mrp.head)
    MapReduceJob(this, phases)
  }
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