package nl.gideondk.raiku.commands

import com.basho.riak.protobuf._
import spray.json._

import play.api.libs.iteratee._
import scala.concurrent.Future

import nl.gideondk.raiku.mapreduce._
import nl.gideondk.raiku.mapreduce.MapReduceJsonProtocol._

import shapeless._
import TypeOperators._
import HList._

import nl.gideondk.raiku.serialization.ProtoBufConversion
import scalaz._
import Scalaz._
import effect._

import scala.concurrent.Promise
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.ByteString

import nl.gideondk.sentinel.client._
import nl.gideondk.sentinel.Task

import shapeless._
import TypeOperators._
import LUBConstraint._
import Tuples._

import akka.actor.ActorRef

private[raiku] case class RiakMROperation(promise: Promise[Unit], channels: List[Concurrent.Channel[JsValue]], command: ByteString)

trait MapReduce extends Connection with ProtoBufConversion with MapReducePoly {
  def mrWorker: ActorRef

  def buildMRRequest(messageType: RiakMessageType, message: ByteString, channels: List[Concurrent.Channel[JsValue]]) =
    Task(mrWorker.sendCommand[Unit, (RiakCommand, List[Concurrent.Channel[JsValue]])](RiakCommand(messageType, message) -> channels).get)

  def buildJobRequest[A <: HList: <<:[MapReducePhase]#λ](job: MapReduceJob[A])(implicit tl: ToList[A, MapReducePhase]) = {
    val jsonJob = JsObject(
      job.input match {
        case x: BucketMapReduceInput    ⇒ "inputs" -> BucketMapReduceInputJsonFormat.write(x)
        case x: ItemMapReduceInput      ⇒ "inputs" -> ItemMapReduceInputJsonFormat.write(x)
        case x: AnnotatedMapReduceInput ⇒ "inputs" -> AnnotatedMapReduceInputJsonFormat.write(x)
        case x: BinIdxMapReduceInput    ⇒ "inputs" -> BinIdxMapReduceInputJsonFormat.write(x)
        case x: IntIdxMapReduceInput    ⇒ "inputs" -> IntIdxMapReduceInputJsonFormat.write(x)
      },
      "query" -> phasesListFormat.write(job.phases.toList),
      "timeout" -> JsNumber(job.timeout.toMillis))

    (RiakMessageType.RpbMapRedReq, RpbMapRedReq(jsonJob.compactPrint, "application/json"))
  }

  def streamMapReduce[A <: HList: <<:[MapReducePhase]#λ, B <: HList, C <: HList, D <: HList, E <: HList, F <: HList, G <: Product](job: MapReduceJob[A])(implicit f1: FilterNotAux[A, NonKeepedMapPhase, B],
                                                                                                                                                         f2: FilterNotAux[B, NonKeepedReducePhase, C],
                                                                                                                                                         l: shapeless.Length[C],
                                                                                                                                                         m1: shapeless.MapperAux[MapReduce.this.newEnumeratorAndChannel.type, C, D],
                                                                                                                                                         m2: shapeless.MapperAux[MapReduce.this.onlyEnumerators.type, D, E],
                                                                                                                                                         m3: shapeless.MapperAux[MapReduce.this.onlyChannels.type, D, F],
                                                                                                                                                         t: TuplerAux[E, G],
                                                                                                                                                         tl: ToList[A, MapReducePhase],
                                                                                                                                                         tl2: ToList[F, Concurrent.Channel[JsValue]]): Task[G] = {
    val phases = job.phases.filterNot[NonKeepedMapPhase].filterNot[NonKeepedReducePhase]
    val req = buildJobRequest(job)
    val channelsAndEnumerators = mrPhasesToBroadcastEnumerators(phases)
    val channels = channelsAndEnumerators.map(onlyChannels).toList
    val enums = channelsAndEnumerators.map(onlyEnumerators).tupled
    buildMRRequest(req._1, req._2, channels).map(_ ⇒ enums)
  }

  def mapReduce[A <: HList: <<:[MapReducePhase]#λ, B <: HList, C <: HList, D <: HList, E <: HList, F <: HList, G <: HList](job: MapReduceJob[A])(implicit f1: FilterNotAux[A, NonKeepedMapPhase, B],
                                                                                                                                                 f2: FilterNotAux[B, NonKeepedReducePhase, C],
                                                                                                                                                 l: shapeless.Length[C],
                                                                                                                                                 m1: shapeless.MapperAux[MapReduce.this.newEnumeratorAndChannel.type, C, D],
                                                                                                                                                 m2: shapeless.MapperAux[MapReduce.this.onlyEnumerators.type, D, E],
                                                                                                                                                 m3: shapeless.MapperAux[MapReduce.this.onlyChannels.type, D, F],
                                                                                                                                                 m4: shapeless.MapperAux[MapReduce.this.consumeEnumerator.type, E, G],
                                                                                                                                                 folder: shapeless.LeftFolder[G, Task[Unit], MapReduce.this.combineFutures.type],
                                                                                                                                                 tl: ToList[A, MapReducePhase],
                                                                                                                                                 tl2: ToList[F, Concurrent.Channel[JsValue]]) = {
    val phases = job.phases.filterNot[NonKeepedMapPhase].filterNot[NonKeepedReducePhase]
    val req = buildJobRequest(job)
    val channelsAndEnumerators = mrPhasesToBroadcastEnumerators(phases)
    val channels = channelsAndEnumerators.map(onlyChannels).toList
    val results = channelsAndEnumerators.map(onlyEnumerators).map(consumeEnumerator)

    results.foldLeft(buildMRRequest(req._1, req._2, channels))(combineFutures)
  }
}

trait MapReducePoly {
  object newEnumeratorAndChannel extends (MapReducePhase -> (Enumerator[JsValue], Concurrent.Channel[JsValue]))(_ ⇒ Concurrent.broadcast[JsValue])

  object onlyEnumerators extends ((Enumerator[JsValue], Concurrent.Channel[JsValue]) -> Enumerator[JsValue])(_._1)

  object onlyChannels extends ((Enumerator[JsValue], Concurrent.Channel[JsValue]) -> Concurrent.Channel[JsValue])(_._2)

  object consumeEnumerator extends (Enumerator[JsValue] -> Future[List[JsValue]])(_ |>>> Iteratee.fold(List.empty[JsValue]) { (result, chunk) ⇒
    result ++ List(chunk)
  })

  def mrPhasesToBroadcastEnumerators[A <: HList, B <: HList](phs: A)(implicit mapper: MapperAux[newEnumeratorAndChannel.type, A, B]) = phs.map(newEnumeratorAndChannel)

  // Phases will return sequentially, so currently no parallel future processing necessary
  object combineFutures extends Poly2 {
    implicit def caseUnitToListFut = at[Task[Unit], Future[List[JsValue]]]((c, s) ⇒ c.flatMap { x ⇒ Task(s.map(y ⇒ (y :: HNil).tupled)) })
    implicit def caseFutToFut[T <: Product, A <: HList, B <: HList](implicit hlister: HListerAux[T, A],
                                                                    prepend: PrependAux[A, shapeless.::[List[spray.json.JsValue], shapeless.HNil], B],
                                                                    tupler: Tupler[B]) =
      at[Task[T], Future[List[JsValue]]]((c, s) ⇒ c.flatMap { x ⇒ Task(s.map(y ⇒ (x.hlisted :+ y).tupled)) })
  }

}