package nl.gideondk.raiku

import scalaz._
import Scalaz._

package object mapreduce {
  implicit def mapPhaseToMapReducePipe(mp: MapPhase): MapReducePipe = MapReducePipe(NonEmptyList(mp))
  implicit def reducePhaseToMapReducePipe(rp: ReducePhase): MapReducePipe = MapReducePipe(NonEmptyList(rp))
}
