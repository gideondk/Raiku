package nl.gideondk.raiku.mapreduce

sealed trait MapReduceFunction {
  def value: String
}

trait BuildInMapReduceFunction

trait MFunction extends MapReduceFunction
trait RFunction extends MapReduceFunction

case class MapFunction(value: String) extends MFunction

case class ReduceFunction(value: String) extends RFunction

case class BuildInMapFunction(value: String) extends MFunction with BuildInMapReduceFunction

case class BuildInReduceFunction(value: String) extends RFunction with BuildInMapReduceFunction