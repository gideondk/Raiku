package nl.gideondk.raiku

import akka.actor._
import commands.RWObject
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

//class ReactiveBucketSpec extends Specification with DefaultJsonProtocol {
//  sequential
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
//  val bucket = RaikuReactiveBucket[Y]("raiku_test_y_bucket_"+java.util.UUID.randomUUID.toString, client)
//
//  val nrOfItems = 500
//
//  val randomObjects = List.fill(nrOfItems)(Y(java.util.UUID.randomUUID.toString, "Test Name", 25, "A")).toList
//  val ids = randomObjects.map(_.id)
//
//  "A reactive bucket" should {
//    "should be able to store objects reactivly" in {
//      val res = Await.result(bucket.store(Enumerator(randomObjects: _*)), Duration(5, SECONDS))
//      res == 500
//    }
//
//    "should be able to delete objects reactivly" in {
//      val res = Await.result(bucket.store(Enumerator(randomObjects: _*)), Duration(5, SECONDS))
//      val delRes = Await.result(bucket.delete(Enumerator(randomObjects: _*)), Duration(5, SECONDS))
//      delRes == 500
//    }
//
//    "should be able to fetch objects based on indexes reactivly" in {
//      Await.result(bucket.store(Enumerator(randomObjects: _*)), Duration(5, SECONDS))
//      val idxs = Vector.fill(5)("group_id" -> "A")
//      val e = Enumerator(idxs: _*) &> bucket.binIdxEnumeratee &> bucket.fetchManyEnumeratee() |>>> Iteratee.fold(List[Y]()) { (result, chunk) ⇒
//        result ++ chunk.flatten
//      }
//
//      val res = Await.result(e, Duration(15, SECONDS))
//      Await.result(bucket.delete(Enumerator(randomObjects: _*)), Duration(5, SECONDS))
//      res.length == 5 * 500
//    }
//
//    "should be able to compose correctly" in {
//      val flow = Enumerator(randomObjects: _*) &> bucket.storeEnumeratee(returnBody = true) &>
//        Enumeratee.filter(_.isDefined) &> Enumeratee.map(x ⇒ x.get) &> bucket.deleteEnumeratee() |>>> Iteratee.fold(0) { (result, chunk) ⇒ result + 1 }
//
//      val res = Await.result(flow, Duration(15, SECONDS))
//      res == 500
//    }
//  }
//}