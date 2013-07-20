package nl.gideondk.raiku.commands

import nl.gideondk.raiku.mapreduce._
import nl.gideondk.raiku.mapreduce.MapReduceJsonProtocol._
import nl.gideondk.raiku.serialization.ProtoBufConversion
import nl.gideondk.sentinel.client._
import nl.gideondk.sentinel.Task
import com.basho.riak.protobuf._
import spray.json._
import play.api.libs.iteratee._
import scala.concurrent.Future
import scalaz._
import Scalaz._
import scala.concurrent.Promise
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.ByteString

import shapeless._
import LUBConstraint._
import Tuples._
import HList._
import nl.gideondk.sentinel.client._
import akka.actor.ActorRef
import akka.pattern._
import akka.actor.Props
import nl.gideondk.raiku.actors._

import Traversables._
import akka.util.Timeout
import scala.concurrent.duration._

trait MapReduce extends Connection with ProtoBufConversion {
  def mrWorker: SentinelClient[RiakCommand, Enumerator[RiakResponse]]

  def buildMRRequest(messageType: RiakMessageType, message: ByteString, phaseCount: Int, maxJobDuration: FiniteDuration = 5 minutes) = {
    (mrWorker <~< RiakCommand(messageType, message)).flatMap { x ⇒
      implicit val timeout = Timeout(maxJobDuration)
      Task((system.actorOf(Props(new MRResultHandler(x, phaseCount, maxJobDuration))) ? StartMRProcessing).mapTo[List[Enumerator[JsValue]]])
    }
  }

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

  def streamMapReduce[A <: HList: <<:[MapReducePhase]#λ, B <: HList, C <: HList, D <: HList, T <: Product, Z <: HList](job: MapReduceJob[A])(implicit f1: FilterNotAux[A, NonKeepedMapPhase, B],
                                                                                                                                             f2: FilterNotAux[B, NonKeepedReducePhase, C],

                                                                                                                                             tl: ToList[A, MapReducePhase],
                                                                                                                                             tl2: ToList[C, nl.gideondk.raiku.mapreduce.MapReducePhase],
                                                                                                                                             mm: MapperAux[phaseToEmptyEnum.type, C, Z],
                                                                                                                                             fl: shapeless.FromTraversable[Z],
                                                                                                                                             t: TuplerAux[Z, T]): Task[T] = {
    val phases = job.phases.filterNot[NonKeepedMapPhase].filterNot[NonKeepedReducePhase]
    val req = buildJobRequest(job)

    val l = phases.toList
    buildMRRequest(req._1, req._2, l.length) map { x ⇒
      x.toHList[Z].getOrElse(throw new Exception("Unexpected number of enumerators returned")).tupled
    }
  }

  def mapReduce[A <: HList: <<:[MapReducePhase]#λ, B <: HList, C <: HList, D <: HList, E <: HList, F <: HList, G <: HList, P <: Product, Z <: HList, Y <: HList, T](job: MapReduceJob[A])(implicit f1: FilterNotAux[A, NonKeepedMapPhase, B],
                                                                                                                                                                                          f2: FilterNotAux[B, NonKeepedReducePhase, C],
                                                                                                                                                                                          tl: ToList[A, MapReducePhase],
                                                                                                                                                                                          tl2: ToList[C, nl.gideondk.raiku.mapreduce.MapReducePhase],
                                                                                                                                                                                          mm: MapperAux[phaseToEmptyEnum.type, C, Z],
                                                                                                                                                                                          fl: shapeless.FromTraversable[Z],

                                                                                                                                                                                          m2: shapeless.MapperAux[MapReduce.this.consumeEnumerator.type, Z, D],
                                                                                                                                                                                          folder: shapeless.LeftFolderAux[D, Task[Unit], MapReduce.this.combineFutures.type, T],
                                                                                                                                                                                          c: MapReduce.this.FlattenTask.Case1[nl.gideondk.sentinel.Task[T]],
                                                                                                                                                                                          l: shapeless.Length[C]) = {
    val phases = job.phases.filterNot[NonKeepedMapPhase].filterNot[NonKeepedReducePhase]
    val req = buildJobRequest(job)
    val l = phases.toList

    FlattenTask(buildMRRequest(req._1, req._2, l.length) map { x ⇒
      val results = x.toHList[Z].getOrElse(throw new Exception("Unexpected number of enumerators returned")).map(consumeEnumerator)
      results.foldLeft(().point[Task])(combineFutures)
    })
  }

  object phaseToEmptyEnum extends (MapReducePhase -> Enumerator[JsValue])(x ⇒ Enumerator[JsValue]())

  object consumeEnumerator extends (Enumerator[JsValue] -> Future[List[JsValue]])(_ |>>> Iteratee.fold(List.empty[JsValue]) {
    (result, chunk) ⇒
      result ++ List(chunk)
  })

  // Phases will return sequentially, so currently no parallel future processing necessary
  object combineFutures extends Poly2 {
    implicit def caseUnitToListFut = at[Task[Unit], Future[List[JsValue]]]((c, s) ⇒ c.flatMap {
      x ⇒ Task(s.map(y ⇒ (y :: HNil).tupled))
    })

    implicit def caseFutToFut[T <: Product, A <: HList, B <: HList](implicit hlister: HListerAux[T, A],
                                                                    prepend: PrependAux[A, shapeless.::[List[spray.json.JsValue], shapeless.HNil], B],
                                                                    tupler: Tupler[B]) =
      at[Task[T], Future[List[JsValue]]]((c, s) ⇒ c.flatMap {
        x ⇒ Task(s.map(y ⇒ (x.hlisted :+ y).tupled))
      })
  }

  object FlattenTask extends Poly1 {
    implicit def default[T] = at[Task[Task[T]]](t ⇒ t.flatMap(x ⇒ x))
  }

}