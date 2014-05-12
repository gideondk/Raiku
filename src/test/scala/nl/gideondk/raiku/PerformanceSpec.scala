//package nl.gideondk.raiku
//
//import akka.actor._
//
//import scala.concurrent._
//import scala.concurrent.duration._
//
//import scalaz._
//import Scalaz._
//
//import spray.json._
//
//import org.specs2.mutable.Specification
//import org.specs2.matcher.Matcher
//
//import scala.concurrent._
//import scala.concurrent.duration._
//import play.api.libs.iteratee.Iteratee
//
//import serialization._
//
//import org.specs2.runner.JUnitRunner
//import org.specs2.mutable.Specification
//import org.specs2.execute.{ Success, Result }
//import org.specs2.specification.{ Fragments, Example }
//
//abstract class BenchmarkSpec extends Specification with RaikuSpec {
//  sequential
//  xonly
//
//  def timed(desc: String, n: Int)(benchmark: ⇒ Unit): Result = {
//    val t = System.currentTimeMillis
//    benchmark
//    val d = System.currentTimeMillis - t
//
//    println(desc+":\n*** number of ops/s: "+n / (d / 1000.0)+"\n")
//    Success()
//  }
//
//}
//
//class PerformanceSpec extends BenchmarkSpec with DefaultJsonProtocol {
//  sequential
//
//  import TestModels._
//
//  val bucket = RaikuBucket[Y]("raiku_test_y_bucket_"+java.util.UUID.randomUUID.toString, client)
//  bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))).copoint
//
//  val reactiveBucket = RaikuReactiveBucket[Y](bucket.bucketName, client)
//
//  val nrOfItems = 500
//
//  val randomObjects = List.fill(nrOfItems)(Y(java.util.UUID.randomUUID.toString, "Test Name", 25, "A")).toList
//  val ids = randomObjects.map(_.id)
//
//  "A bucket" should {
//    "be able to store objects in timely fashion" in {
//      val acts = randomObjects.map(x ⇒ bucket << (x, w = 1))
//
//      timed("Storing "+nrOfItems+" items sequentially", nrOfItems) {
//        acts.foreach { x ⇒
//          x.run(Duration(30, SECONDS))
//        }
//      }
//
//      val futs = bucket <<* (randomObjects, w = 1)
//      timed("Storing "+nrOfItems+" items in parallel", nrOfItems) {
//        val status = futs.run(Duration(30, SECONDS))
//      }
//    }
//
//    "be able to fetch objects in timely fashion" in {
//      val acts = ids.map(x ⇒ bucket ? (x, r = 1))
//
//      timed("Fetching "+nrOfItems+" items sequentially", nrOfItems) {
//        acts.foreach { x ⇒
//          x.run(Duration(30, SECONDS))
//        }
//      }
//
//      val futs = bucket ?* (ids, r = 1)
//
//      timed("Fetching "+nrOfItems+" items in parallel", nrOfItems) {
//        val status = futs.run(Duration(30, SECONDS))
//      }
//    }
//
//    "be able to fetch keys on indexes in timely fashion" in {
//      val acts = for (i ← 0 to 100) yield { bucket idx ("age", 25) }
//      timed("Fetching "+nrOfItems+" index keys sequentially", nrOfItems * 100) {
//        acts.foreach { x ⇒
//          x.run(Duration(30, SECONDS))
//        }
//      }
//
//      val parActs = for (i ← 0 to 100) yield { bucket idx ("age", 25) }
//      val seq = Task.sequenceSuccesses(parActs.toList)
//      timed("Fetching "+nrOfItems+" index keys in parallel", nrOfItems * 100) {
//        seq.run(Duration(30, SECONDS))
//      }
//    }
//
//    "be able to stream index keys in timely fashion" in {
//      import play.api.libs.iteratee
//      val parActs = for (i ← 0 to 5) yield { (bucket streamIdx ("age", 25)).flatMap(x ⇒ Task(x |>>> Iteratee.getChunks)) }
//
//      val seq = Task.sequenceSuccesses(parActs.toList)
//      timed("Streaming "+nrOfItems+" index keys in parallel", nrOfItems * 5) {
//        val a = seq.run(Duration(30, SECONDS))
//      }
//    }
//
//    "be able to stream index keys into a fetch in timely fashion" in {
//      import play.api.libs.iteratee
//      val parActs = for (i ← 0 to 5) yield { (bucket streamIdx ("age", 25)).flatMap(x ⇒ Task(x &> reactiveBucket.fetchEnumeratee() |>>> Iteratee.getChunks)) }
//
//      val seq = Task.sequenceSuccesses(parActs.toList)
//      timed("Streaming "+nrOfItems+" items from indexes in parallel", nrOfItems * 5) {
//        val a = seq.run(Duration(30, SECONDS))
//
//      }
//    }
//
//    "be able to delete objects in timely fashion" in {
//      val futs = bucket -* (randomObjects, r = 1, w = 1)
//
//      timed("Deleting "+nrOfItems+" items in parallel", nrOfItems) {
//        val status = futs.run(Duration(30, SECONDS))
//      }
//    }
//  }
//
//}
//
