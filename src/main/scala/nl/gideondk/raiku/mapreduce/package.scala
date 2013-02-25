package nl.gideondk.raiku

import nl.gideondk.raiku.mapreduce.{ BuildInMapFunction, BuildInReduceFunction, MapFunction, ReduceFunction }
import shapeless.HNil

package object mapreduce {
  implicit def mapFunctionToMapReducePhases(mf: MapFunction) = MapReducePhases(HNil, NonKeepedMapPhase(mf))

  implicit def buildInMapFunctionToMapReducePhases(mf: BuildInMapFunction) = MapReducePhases(HNil, NonKeepedMapPhase(mf))

  implicit def reduceFunctionToMapReducePhases(rf: ReduceFunction) = MapReducePhases(HNil, NonKeepedReducePhase(rf))

  implicit def buildInReduceFunctionToMapReducePhases(rf: BuildInReduceFunction) = MapReducePhases(HNil, NonKeepedReducePhase(rf))
}
