package nl.gideondk.raiku

import akka.actor._
import commands.RaikuRWObject
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import spray.json._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

import scala.concurrent._
import scala.concurrent.duration._

class PerformanceSpec extends BenchmarkSpec with DefaultJsonProtocol {
  sequential

  implicit val system = ActorSystem("perf-bucket-system")
  val client = RaikuClient("localhost", 8087, 4)

  implicit val yFormat = jsonFormat4(Y)

  implicit val yConverter = new RaikuConverter[Y] {
    def read(o: RaikuRWObject): ReadResult[Y] = try {
      yFormat.read(new String(o.value).asJson).success
    }
    catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: Y): RaikuRWObject = RaikuRWObject(bucket, o.id, o.toJson.toString.getBytes,
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

      timed("Storing "+nrOfItems+" items sequentially (bad)", nrOfItems) {
        acts.foreach { x ⇒
          x.unsafeFulFill(Duration(15, SECONDS))
        }
      }

      val futs = bucket <<* (randomObjects, w = 1)
      timed("Storing "+nrOfItems+" items in parallel (good)", nrOfItems) {
        val status = futs.unsafeFulFill(Duration(15, SECONDS))
      }
    }

    "be able to fetch objects in timely fashion" in {
      val acts = ids.map(x ⇒ bucket ? (x, r = 1))

      timed("Fetching "+nrOfItems+" items sequentially (bad)", nrOfItems) {
        acts.foreach { x ⇒
          x.unsafeFulFill(Duration(15, SECONDS))
        }
      }

      val futs = bucket ?* (ids, r = 1)

      timed("Fetching "+nrOfItems+" items in parallel (good)", nrOfItems) {
        val status = futs.unsafeFulFill(Duration(15, SECONDS))
      }
    }
    "be able to delete objects in timely fashion" in {

      val futs = bucket -* (randomObjects, r = 1, w = 1)

      timed("Deleting "+nrOfItems+" items in parallel", nrOfItems) {
        val status = futs.unsafeFulFill(Duration(15, SECONDS))
      }
    }
  }

  step {
    client.disconnect
    system.shutdown()
  }
}