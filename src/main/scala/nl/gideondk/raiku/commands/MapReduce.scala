package nl.gideondk.raiku.commands

import nl.gideondk.raiku.mapreduce._
import nl.gideondk.raiku.mapreduce.MapReduceJsonProtocol._
import nl.gideondk.raiku.serialization.ProtoBufConversion

import com.basho.riak.protobuf._
import spray.json._
import play.api.libs.iteratee._
import scala.concurrent.Future
import scala.concurrent.Promise
import akka.util.ByteString
import akka.actor.ActorRef
import akka.pattern._
import akka.actor.Props
import nl.gideondk.raiku.actors._
import akka.util.Timeout
import scala.concurrent.duration._

trait MapReduce extends Connection with ProtoBufConversion {

  def buildMRRequest(messageType: RiakMessageType, message: ByteString, phaseCount: Int, maxJobDuration: FiniteDuration = 5 minutes) = {
    (worker ?->> RiakCommand(messageType, message)).flatMap { x ⇒
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

  def streamMapReduce[A <: HList: <<:[MapReducePhase]#λ, B <: HList, C <: HList, D <: HList, T <: Product, Z <: HList](job: MapReduceJob[A])(implicit f1: FilterNot.Aux[A, NonKeepedMapPhase, B],
                                                                                                                                             f2: FilterNot.Aux[B, NonKeepedReducePhase, C],

                                                                                                                                             tl: ToList[A, MapReducePhase],
                                                                                                                                             tl2: ToList[C, nl.gideondk.raiku.mapreduce.MapReducePhase],
                                                                                                                                             mm: Mapper.Aux[phaseToEmptyEnum.type, C, Z],
                                                                                                                                             fl: shapeless.ops.traversable.FromTraversable[Z],
                                                                                                                                             t: Tupler.Aux[Z, T]): Task[T] = {
    val phases = job.phases.filterNot[NonKeepedMapPhase].filterNot[NonKeepedReducePhase]
    val req = buildJobRequest(job)

    val l = phases.toList
    buildMRRequest(req._1, req._2, l.length) map { x ⇒
      fl(x).getOrElse(throw new Exception("Unexpected number of enumerators returned")).tupled
    }
  }

  def mapReduce[A <: HList: <<:[MapReducePhase]#λ, B <: HList, C <: HList, D <: HList, E <: HList, F <: HList, G <: HList, P <: Product, Z <: HList, Y <: HList, T](job: MapReduceJob[A])(implicit f1: FilterNot.Aux[A, NonKeepedMapPhase, B],
                                                                                                                                                                                          f2: FilterNot.Aux[B, NonKeepedReducePhase, C],
                                                                                                                                                                                          tl: ToList[A, MapReducePhase],
                                                                                                                                                                                          tl2: ToList[C, nl.gideondk.raiku.mapreduce.MapReducePhase],
                                                                                                                                                                                          mm: Mapper.Aux[phaseToEmptyEnum.type, C, Z],
                                                                                                                                                                                          fl: shapeless.ops.traversable.FromTraversable[Z],

                                                                                                                                                                                          m2: Mapper.Aux[MapReduce.this.consumeEnumerator.type, Z, D],
                                                                                                                                                                                          folder: LeftFolder.Aux[D, Task[Unit], MapReduce.this.combineFutures.type, T],
                                                                                                                                                                                          c: MapReduce.this.FlattenTask.Case[nl.gideondk.sentinel.Task[T]]) = {
    val phases = job.phases.filterNot[NonKeepedMapPhase].filterNot[NonKeepedReducePhase]
    val req = buildJobRequest(job)
    val l = phases.toList

    FlattenTask(buildMRRequest(req._1, req._2, l.length) map { x ⇒
      val results = fl(x).getOrElse(throw new Exception("Unexpected number of enumerators returned")).map(consumeEnumerator)
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

    implicit def caseFutToFut[T <: Product, A <: HList, B <: HList](implicit hlister: Generic.Aux[T, A],
                                                                    prepend: Prepend.Aux[A, shapeless.::[List[spray.json.JsValue], shapeless.HNil], B],
                                                                    tupler: Tupler[B]) =
      at[Task[T], Future[List[JsValue]]]((c, s) ⇒ c.flatMap {
        x ⇒ Task(s.map(y ⇒ (hlister.to(x) :+ y).tupled))
      })
  }

  object FlattenTask extends Poly1 {
    implicit def default[T] = at[Task[Task[T]]](t ⇒ t.flatMap(x ⇒ x))
  }

}