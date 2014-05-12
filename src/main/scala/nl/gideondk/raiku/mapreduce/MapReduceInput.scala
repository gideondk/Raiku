package nl.gideondk.raiku.mapreduce

trait MapReduceInput {
  import MapReducePhases._

  def |>>(f: MFunction) =
    MapReduceJob(this, MapPhase(f) :: HNil)

  def |>>(f: RFunction) =
    MapReduceJob(this, ReducePhase(f) :: HNil)

  def |>>[T <: HList: <<:[MapReducePhase]#λ, Z <: HList: <<:[MapReducePhase]#λ](mrp: MapReducePhasesMHead[T])(implicit p: Prepend.Aux[T, MapPhase :: HNil, Z]) = {
    val phases = mrp.phases :+ keeped(mrp.head)
    MapReduceJob(this, phases)
  }

  def |>>[T <: HList: <<:[MapReducePhase]#λ, Z <: HList: <<:[MapReducePhase]#λ](mrp: MapReducePhasesRHead[T])(implicit p: Prepend.Aux[T, ReducePhase :: HNil, Z]) = {
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