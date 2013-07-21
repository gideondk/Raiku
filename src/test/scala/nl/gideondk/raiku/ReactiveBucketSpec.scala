package nl.gideondk.raiku

import akka.actor._
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import scalaz._
import Scalaz._

import spray.json._

import play.api.libs.iteratee._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

import scala.concurrent.ExecutionContext.Implicits.global

class ReactiveBucketSpec extends RaikuSpec {
  import TestModels._
  sequential

  val bucket = RaikuReactiveBucket[Y]("raiku_test_y_bucket_"+java.util.UUID.randomUUID.toString, client)

  val normalBucket = RaikuBucket[Y](bucket.bucketName, client)

  val nrOfItems = 100

  val randomObjects = List.fill(nrOfItems)(Y(java.util.UUID.randomUUID.toString, "Test Name", scala.util.Random.nextInt(100), "A")).toList
  val ids = randomObjects.map(_.id)

  "A reactive bucket" should {
    "should be able to store objects reactivly" in {
      val res = Await.result(bucket.store(Enumerator(randomObjects: _*)), Duration(15, SECONDS))
      res == nrOfItems
    }

    "should be able to delete objects reactivly" in {
      val res = Await.result(bucket.store(Enumerator(randomObjects: _*)), Duration(15, SECONDS))
      val delRes = Await.result(bucket.delete(Enumerator(randomObjects: _*)), Duration(15, SECONDS))
      delRes == nrOfItems
    }

    "should be able to fetch objects based on indexes reactivly" in {
      Await.result(bucket.store(Enumerator(randomObjects: _*)), Duration(15, SECONDS))
      val idxs = Vector.fill(5)("group_id" -> "A")

      val e = bucket.client.streamKeysForBinIndexByValue(bucket.bucketName, "group_id", "A").copoint &> bucket.fetchEnumeratee() |>>> Iteratee.fold(List[Y]()) { (result, chunk) ⇒
        result ++ chunk.toList
      }

      val res = Await.result(e, Duration(30, SECONDS))
      Await.result(bucket.delete(Enumerator(randomObjects: _*)), Duration(15, SECONDS))
      res.length == nrOfItems
    }

    "should be able to compose correctly" in {
      val flow = Enumerator(randomObjects: _*) &> bucket.storeEnumeratee(returnBody = true) &>
        Enumeratee.filter(_.isDefined) &> Enumeratee.map(x ⇒ x.get) &> bucket.deleteEnumeratee() |>>> Iteratee.fold(0) { (result, chunk) ⇒ result + 1 }

      val res = Await.result(flow, Duration(30, SECONDS))
      res == nrOfItems
    }
  }
}
