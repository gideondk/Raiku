package nl.gideondk.raiku

import akka.actor._
import commands.RWObject
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import spray.json._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

import scala.concurrent._
import scala.concurrent.duration._
import play.api.libs.iteratee.Iteratee

import serialization._
import monads._

class PerformanceSpec extends BenchmarkSpec with DefaultJsonProtocol {
  sequential

  implicit val system = ActorSystem("perf-bucket-system")
  val client = RaikuClient("localhost", 8087, 4)

  implicit val yFormat = jsonFormat4(Y)

  implicit val yConverter = new RaikuConverter[Y] {
    def read(o: RWObject): ReadResult[Y] = try {
      yFormat.read(new String(o.value).asJson).success
    }
    catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: Y): RWObject = RWObject(bucket, o.id, o.toJson.toString.getBytes,
      binIndexes = Map("group_id" -> List(o.groupId)), intIndexes = Map("age" -> List(o.age)))
  }

  val nrOfItems = 2000

  val randomObjects = List.fill(nrOfItems)(Y(java.util.UUID.randomUUID.toString, "Test Name", 25, "A")).toList
  val ids = randomObjects.map(_.id)

  val bucket = RaikuBucket[Y]("raiku_test_y_bucket", client)
  bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))).unsafeFulFill

  "A bucket" should {
    "be able to store objects in timely fashion" in {
      val acts = randomObjects.map(x ⇒ bucket << (x, w = 1))

      timed("Storing "+nrOfItems+" items sequentially", nrOfItems) {
        acts.foreach { x ⇒
          x.unsafeFulFill(Duration(15, SECONDS))
        }
      }

      val futs = bucket <<* (randomObjects, w = 1)
      timed("Storing "+nrOfItems+" items in parallel", nrOfItems) {
        val status = futs.unsafeFulFill(Duration(15, SECONDS))
      }
    }

    "be able to fetch objects in timely fashion" in {
      val acts = ids.map(x ⇒ bucket ? (x, r = 1))

      timed("Fetching "+nrOfItems+" items sequentially", nrOfItems) {
        acts.foreach { x ⇒
          x.unsafeFulFill(Duration(15, SECONDS))
        }
      }

      val futs = bucket ?* (ids, r = 1)

      timed("Fetching "+nrOfItems+" items in parallel", nrOfItems) {
        val status = futs.unsafeFulFill(Duration(15, SECONDS))
      }
    }

    "be able to fetch keys on indexes in timely fashion" in {
      val acts = for (i ← 0 to 100) yield { bucket idx ("age", 25) }
      timed("Fetching "+nrOfItems+" index keys sequentially", nrOfItems * 100) {
        acts.foreach { x ⇒
          x.unsafeFulFill(Duration(15, SECONDS))
        }
      }

      val parActs = for (i ← 0 to 100) yield { bucket idx ("age", 25) }
      val seq = ValidatedFutureIO.sequence(parActs.toList)
      timed("Fetching "+nrOfItems+" index keys in parallel", nrOfItems * 100) {
        seq.unsafeFulFill(Duration(15, SECONDS))
      }
    }

    "be able to delete objects in timely fashion" in {

      val futs = bucket -* (randomObjects, r = 1, w = 1)

      timed("Deleting "+nrOfItems+" items in parallel", nrOfItems) {
        val status = futs.unsafeFulFill(Duration(15, SECONDS))
      }
    }
  }

}