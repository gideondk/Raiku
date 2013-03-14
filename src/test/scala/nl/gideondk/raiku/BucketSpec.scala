package nl.gideondk.raiku

import akka.actor._
import commands.RWObject
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

case class Z(id: String, name: String)

class BucketSpec extends Specification {
  val client = DB.client

  implicit val zConverter = new RaikuConverter[Z] {
    def read(o: RWObject): ReadResult[Z] = Z(o.key, new String(o.value)).success
    def write(bucket: String, o: Z): RWObject = RWObject(bucket, o.id, o.name.getBytes)
  }

  val bucket = RaikuBucket[Z]("raiku_test_z_bucket", client)
  bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))).unsafeFulFill

  "A bucket" should {
    "be able to store objects" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")
      val v = bucket << obj
      v.unsafeFulFill.isSuccess
    }
    "be able to fetch stored objects" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")

      val retObj = for {
        v ← bucket unsafeStoreNew obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      retObj.unsafeFulFill.toOption.get.get == obj
    }
    "create siblings (and fail) when unsafely updating objects" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")

      val retObj = for {
        v ← bucket unsafeStoreNew obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      retObj.unsafeFulFill

      val updatedObj = for {
        v ← bucket unsafeStoreNew obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      updatedObj.unsafeFulFill.isFailure
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

      retObj.unsafeFulFill

      val updatedObj = for {
        v ← bucket << obj
        retObj ← bucket ? obj.id
      } yield {
        retObj
      }

      updatedObj.unsafeFulFill.isSuccess
    }

    "be able to persist multiple objects" in {
      val vec = List.fill(50)(Z(java.util.UUID.randomUUID.toString, "Should also be persisted"))
      val retObj = for {
        vs ← bucket <<* vec
        retObj ← bucket ?* vec.map(_.id)
      } yield retObj

      val res = retObj.unsafeFulFill
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

      val res = retObj.unsafeFulFill
      res.isSuccess && res.toOption.get == true
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

      val res = retObj.unsafeFulFill
      res.isSuccess && res.toOption.get == true
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

      val res = retObj.unsafeFulFill
      res.isSuccess && res.toOption.get == true
    }
  }
}