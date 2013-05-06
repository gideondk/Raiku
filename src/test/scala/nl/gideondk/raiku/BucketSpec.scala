package nl.gideondk.raiku

import akka.actor._
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

import scala.util.{ Success, Failure }

case class Z(id: String, name: String)

class BucketSpec extends Specification {
  val client = DB.client

  implicit val timeout = Duration(5, duration.SECONDS)

  implicit val zConverter = RaikuConverter.newConverter(
    reader = (v: RaikuRWValue) ⇒ Z(v.key, new String(v.data)),
    writer = (o: Z) ⇒ RaikuRWValue(o.id, o.name.getBytes(), "application/json"))

  val bucket = RaikuBucket[Z]("raiku_test_z_bucket", client)
  bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))).copoint

  "A bucket" should {
    "be able to store objects" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")
      val v = bucket << obj
      v.run.isSuccess
    }
    "be able to fetch stored objects" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")

      val retObj = for {
        v ← bucket << obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      val res: Z = retObj.copoint.get
      res == obj
    }
    "create siblings (and fail) when unsafely updating objects" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")

      val retObj = for {
        v ← bucket unsafeStoreNew obj
        v ← bucket unsafeStoreNew obj
        v ← bucket unsafeStoreNew obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      retObj.run

      val updatedObj = for {
        v ← bucket unsafeStoreNew obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      updatedObj.run.isFailure
    }
    "shouldn't create siblings when updating safely" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")

      val retObj = for {
        v ← bucket << obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      retObj.copoint

      val updatedObj = for {
        v ← bucket << obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      updatedObj.run.isSuccess
    }

    "be able to persist multiple objects" in {
      val vec = List.fill(50)(Z(java.util.UUID.randomUUID.toString, "Should also be persisted"))
      val retObj = for {
        vs ← bucket <<* vec
        retObj ← bucket ?* vec.map(_.id)
      } yield retObj

      val res = retObj.run
      res.isSuccess && res.toOption.get.length == vec.length
    }

    "be able to delete objects correctly" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")

      val retObj = for {
        v ← bucket << obj
        firstRet ← bucket ? obj.id
        _ ← bucket - obj
        secRet ← bucket ? obj.id
      } yield {
        firstRet.isDefined && !secRet.isDefined
      }

      val res = retObj.run

      res.isSuccess && res.toOption.get
    }

    "be able to delete objects correctly by key" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")

      val retObj = for {
        v ← bucket << obj
        firstRet ← bucket ? obj.id
        _ ← bucket deleteByKey obj.id
        secRet ← bucket ? obj.id
      } yield {
        firstRet.isDefined && !secRet.isDefined
      }

      val res = retObj.run
      res.isSuccess && res.toOption.get
    }

    "shouldn't be able to fetch multiple deleted objects" in {
      val vec = List.fill(50)(Z(java.util.UUID.randomUUID.toString, "Should also be persisted"))
      val retObj = for {
        vs ← bucket <<* vec
        bef ← bucket ?* vec.map(_.id)
        _ ← bucket -* vec
        aft ← bucket ?* vec.map(_.id)
      } yield {
        bef.length == 50 && aft.length == 0
      }

      val res = retObj.run
      res.isSuccess && res.toOption.get
    }
  }
}