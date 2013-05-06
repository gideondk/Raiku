package nl.gideondk.raiku

import mapreduce._
import scalaz._
import scala.concurrent.Await
import Scalaz._
import spray.json._
import org.specs2.mutable.Specification
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.util.Random
import play.api.libs.iteratee.{ Enumerator, Iteratee }

import shapeless._
import HList._
import Typeable._
import Traversables._

//class MapReduceSpec extends Specification with DefaultJsonProtocol {
//  import nl.gideondk.raiku.mapreduce.MapReduceJsonProtocol._
//  sequential
//
//  def typed[T](t: ⇒ T) {}
//
//  val client = DB.client
//
//  implicit val yFormat = jsonFormat4(Y)
//
//  implicit val yConverter = new RaikuConverter[Y] {
//    def read(o: RWObject): ReadResult[Y] = try {
//      yFormat.read(new String(o.value).asJson).success
//    }
//    catch {
//      case e: Throwable ⇒ e.failure
//    }
//    def write(bucket: String, o: Y): RWObject = RWObject(bucket, o.id, o.toJson.toString.getBytes,
//      binIndexes = Map("group_id" -> List(o.groupId)), intIndexes = Map("age" -> List(o.age)))
//  }
//
//  val bucket = RaikuBucket[Y]("raiku_test_z_bucket_"+java.util.UUID.randomUUID.toString, client)
//
//  import scala.concurrent.ExecutionContext.Implicits.global
//  val duration = scala.concurrent.duration.pairIntToDuration((60, scala.concurrent.duration.SECONDS))
//  val groupIds = Vector.fill(10)(java.util.UUID.randomUUID.toString)
//  val n = 5000
//  val rnd = new scala.util.Random
//  val vec = List.fill(n)(Y.apply(java.util.UUID.randomUUID.toString, "NAME", rnd.nextInt(99), Random.shuffle(groupIds).head))
//  (bucket <<* vec).unsafeFulFill(duration)
//
//  "A map reduce phases" should {
//    "be build correctly" in {
//      val a = BuildInMapFunction("Riak.mapValuesJson")
//      val b = BuildInMapFunction("Riak.filterNotFound")
//      val c = BuildInReduceFunction("Riak.reduceSum")
//      val d = BuildInReduceFunction("Riak.reduceMin")
//
//      val eee = MapReducePhases(HNil, NonKeepedMapPhase(a))
//
//      val mrJob = MR.items(Set(("persons", "a"), ("persons", "b"))) |>> a >-> c
//
//      typed[NonKeepedMapPhase :: ReducePhase :: HNil](mrJob.phases)
//
//      buildInMapFunctionToMapReducePhases(a)
//
//      val r = MR.items(Set(("persons", "a"), ("persons", "b"))) |>> buildInMapFunctionToMapReducePhases(a) >-> b >=> c >-> d
//
//      typed[NonKeepedMapPhase :: MapPhase :: NonKeepedReducePhase :: ReducePhase :: HNil](r.phases)
//      true
//    }
//  }
//
//  "A bucket based MR job" should {
//    "return the correct results" in {
//      val mapToNumber = MapFunction("function() {return [1]; }")
//      val reduceSum = BuildInReduceFunction("Riak.reduceSum")
//
//      val mrJob = MR.bucket(bucket.bucketName) |>> mapToNumber >=> reduceSum
//      val phases = mrJob.phases
//
//      val res = (client mapReduce mrJob).unsafeFulFill(duration) match {
//        case Failure(e) ⇒ throw e
//        case Success(r) ⇒ r._1.length == n && r._2(0) == JsNumber(n)
//      }
//    }
//  }
//
//  "A input based MR job" should {
//    "return the correct results" in {
//
//      val mapToNumber = MapFunction("function() {return [1]; }")
//      val reduceSum = BuildInReduceFunction("Riak.reduceSum")
//
//      val mrJob = MR.items(Set((bucket.bucketName, vec(0).id), (bucket.bucketName, vec(1).id))) |>> mapToNumber >=> reduceSum
//      val phases = mrJob.phases
//
//      val res = (client mapReduce mrJob).unsafeFulFill(duration) match {
//        case Failure(e) ⇒ throw e
//        case Success(r) ⇒ r._1.length == 2 && r._2(0) == JsNumber(2)
//      }
//    }
//  }
//
//  "A idx based MR job" should {
//    "return the correct results for binary indexes" in {
//
//      val mapToNumber = MapFunction("function() {return [1]; }")
//      val reduceSum = BuildInReduceFunction("Riak.reduceSum")
//
//      val mrJob = MR.binIdx(bucket.bucketName, "group_id", groupIds(0)) |>> mapToNumber >=> reduceSum
//      val phases = mrJob.phases
//
//      val correctItems = vec.filter(_.groupId == groupIds(0))
//
//      val res = (client mapReduce mrJob).unsafeFulFill(duration) match {
//        case Failure(e) ⇒ throw e
//        case Success(r) ⇒ r._1.length == correctItems.length && r._2(0) == JsNumber(correctItems.length)
//      }
//    }
//
//    "return the correct results for int indexes" in {
//
//      val mapToNumber = MapFunction("function() {return [1]; }")
//      val reduceSum = BuildInReduceFunction("Riak.reduceSum")
//
//      val mrJob = MR.intIdx(bucket.bucketName, "age", 22) |>> mapToNumber >=> reduceSum
//      val phases = mrJob.phases
//
//      val correctItems = vec.filter(_.age == 22)
//
//      val res = (client mapReduce mrJob).unsafeFulFill(duration) match {
//        case Failure(e) ⇒ throw e
//        case Success(r) ⇒ r._1.length == correctItems.length && r._2(0) == JsNumber(correctItems.length)
//      }
//    }
//
//    "return the correct results for range indexes" in {
//
//      val mapToNumber = MapFunction("function() {return [1]; }")
//      val reduceSum = BuildInReduceFunction("Riak.reduceSum")
//
//      val mrJob = MR.rangeIdx(bucket.bucketName, "age", 50 to 70) |>> mapToNumber >=> reduceSum
//      val phases = mrJob.phases
//
//      val correctItems = vec.filter(x ⇒ x.age >= 50 && x.age <= 70)
//
//      val res = (client mapReduce mrJob).unsafeFulFill(duration) match {
//        case Failure(e) ⇒ throw e
//        case Success(r) ⇒ r._1.length == correctItems.length && r._2(0) == JsNumber(correctItems.length)
//      }
//    }
//  }
//}