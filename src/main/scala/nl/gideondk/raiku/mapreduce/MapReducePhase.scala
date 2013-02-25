package nl.gideondk.raiku.mapreduce

import scalaz.Scalaz._
import shapeless.{ :: ⇒ ::, HList }
import shapeless.{ HNil, Poly1, Prepend }
import shapeless.HList.hlistOps
import spray.json.{ JsValue, JsonWriter }

object MapReducePhases {
  def apply[T <: HList](p: T, h: NonKeepedMapPhase) = new MapReducePhasesMHead[T] {
    val phases = p
    val head = h
  }

  def apply[T <: HList](p: T, h: NonKeepedReducePhase) = new MapReducePhasesRHead[T] {
    val phases = p
    val head = h
  }

  object keeped extends Poly1 {
    implicit def caseMapPhase = at[NonKeepedMapPhase] { x ⇒
      MapPhase(x.fn, x.args)
    }
    implicit def caseReducePhase = at[NonKeepedReducePhase] { x ⇒
      ReducePhase(x.fn, x.args)
    }
  }
}

trait MapReducePhasesRHead[T <: HList] extends MapReducePhases[T, NonKeepedMapPhase] {
  import MapReducePhases._
  import Prepend._

  def head: NonKeepedReducePhase

  def >->(fun: MFunction)(implicit p: Prepend[T, NonKeepedReducePhase :: HNil]) = MapReducePhases(phases :+ head, NonKeepedMapPhase(fun))
  def >->(fun: RFunction)(implicit p: Prepend[T, NonKeepedReducePhase :: HNil]) = MapReducePhases(phases :+ head, NonKeepedReducePhase(fun))

  def >->[A](fun: MFunction, args: A)(implicit writer: JsonWriter[A], p: Prepend[T, NonKeepedReducePhase :: HNil]) = MapReducePhases(phases :+ head, NonKeepedMapPhase(fun, writer.write(args).point[Option]))
  def >->[A](fun: RFunction, args: A)(implicit writer: JsonWriter[A], p: Prepend[T, NonKeepedReducePhase :: HNil]) = MapReducePhases(phases :+ head, NonKeepedReducePhase(fun, writer.write(args).point[Option]))

  def >=>(fun: MFunction)(implicit p: Prepend[T, ReducePhase :: HNil]) = MapReducePhases(phases :+ keeped(head), NonKeepedMapPhase(fun))
  def >=>(fun: RFunction)(implicit p: Prepend[T, ReducePhase :: HNil]) = MapReducePhases(phases :+ keeped(head), NonKeepedReducePhase(fun))

  def >=>[A](fun: MFunction, args: A)(implicit writer: JsonWriter[A], p: Prepend[T, ReducePhase :: HNil]) = MapReducePhases(phases :+ keeped(head), NonKeepedMapPhase(fun, writer.write(args).point[Option]))
  def >=>[A](fun: RFunction, args: A)(implicit writer: JsonWriter[A], p: Prepend[T, ReducePhase :: HNil]) = MapReducePhases(phases :+ keeped(head), NonKeepedReducePhase(fun, writer.write(args).point[Option]))
}

trait MapReducePhasesMHead[T <: HList] extends MapReducePhases[T, NonKeepedMapPhase] {
  import MapReducePhases._
  import Prepend._

  def head: NonKeepedMapPhase

  def >->(fun: MFunction)(implicit p: Prepend[T, NonKeepedMapPhase :: HNil]) = MapReducePhases(phases :+ head, NonKeepedMapPhase(fun))
  def >->(fun: RFunction)(implicit p: Prepend[T, NonKeepedMapPhase :: HNil]) = MapReducePhases(phases :+ head, NonKeepedReducePhase(fun))

  def >->[A](fun: MFunction, args: A)(implicit writer: JsonWriter[A], p: Prepend[T, NonKeepedMapPhase :: HNil]) = MapReducePhases(phases :+ head, NonKeepedMapPhase(fun, writer.write(args).point[Option]))
  def >->[A](fun: RFunction, args: A)(implicit writer: JsonWriter[A], p: Prepend[T, NonKeepedMapPhase :: HNil]) = MapReducePhases(phases :+ head, NonKeepedReducePhase(fun, writer.write(args).point[Option]))

  def >=>(fun: MFunction)(implicit p: Prepend[T, MapPhase :: HNil]) = MapReducePhases(phases :+ keeped(head), NonKeepedMapPhase(fun))
  def >=>(fun: RFunction)(implicit p: Prepend[T, MapPhase :: HNil]) = MapReducePhases(phases :+ keeped(head), NonKeepedReducePhase(fun))

  def >=>[A](fun: MFunction, args: A)(implicit writer: JsonWriter[A], p: Prepend[T, MapPhase :: HNil]) = MapReducePhases(phases :+ keeped(head), NonKeepedMapPhase(fun, writer.write(args).point[Option]))
  def >=>[A](fun: RFunction, args: A)(implicit writer: JsonWriter[A], p: Prepend[T, MapPhase :: HNil]) = MapReducePhases(phases :+ keeped(head), NonKeepedReducePhase(fun, writer.write(args).point[Option]))
}

trait MapReducePhases[T <: HList, H] {
  import MapReducePhases._
  import Prepend._

  def phases: T
}

trait MapReducePhase {
  def fn: MapReduceFunction

  def args: Option[JsValue]
}

trait MPhase extends MapReducePhase

trait RPhase extends MapReducePhase

trait NonKeepedMapReducePhase

case class MapPhase(fn: MFunction, args: Option[JsValue] = None) extends MPhase

case class ReducePhase(fn: RFunction, args: Option[JsValue] = None) extends RPhase

case class NonKeepedMapPhase(fn: MFunction, args: Option[JsValue] = None) extends MPhase with NonKeepedMapReducePhase

case class NonKeepedReducePhase(fn: RFunction, args: Option[JsValue] = None) extends RPhase with NonKeepedMapReducePhase
