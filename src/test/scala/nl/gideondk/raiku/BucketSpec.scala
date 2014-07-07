package nl.gideondk.raiku

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

class BucketSpec extends RaikuSpec {

  import nl.gideondk.raiku.TestModels._

  val bucket = RaikuBucket.default[Z]("raiku_test_z_bucket", client)

  Await.result(bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))), 5 seconds)

  "A bucket" should {
    "be able to store objects" in {
      val newId = java.util.UUID.randomUUID.toString
      val obj = Z(newId, "Should also be stored")
      val v = bucket.store(obj, true)
      v.futureValue should equal(Some(obj))
    }
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

    retObj.futureValue should equal(Some(obj))
  }
  "create siblings (and fail) when unsafely updating objects" in {
    val newId = java.util.UUID.randomUUID.toString
    val obj = Z(newId, "Should also be stored")

    val retObj = for {
      v ← client unsafeStoreNew zConverter.write(bucket.bucketName, bucket.bucketType, obj)
      v ← client unsafeStoreNew zConverter.write(bucket.bucketName, bucket.bucketType, obj)
      v ← client unsafeStoreNew zConverter.write(bucket.bucketName, bucket.bucketType, obj)
      retObj ← bucket ? obj.id
    } yield {
      ()
    }

    an[UnresolvedSiblingsConflict] should be thrownBy Await.result(retObj, 5 seconds)
  }

  "shouldn't create siblings when updating safely" in {
    val newId = java.util.UUID.randomUUID.toString
    val obj = Z(newId, "Should also be stored")

    val retObj = for {
      v ← bucket << obj
      v ← bucket << obj
      v ← bucket << obj
      retObj ← bucket ? obj.id
    } yield {
      retObj
    }

    retObj.futureValue should equal(Some(obj))
  }

  "be able to persist multiple objects" in {
    val vec = List.fill(50)(Z(java.util.UUID.randomUUID.toString, "Should also be persisted"))
    val retObj = for {
      vs ← bucket <<* vec
      retObj ← bucket ?* vec.map(_.id)
    } yield retObj

    retObj.futureValue should contain theSameElementsAs (vec)
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

    retObj.futureValue should be(true)
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

    retObj.futureValue should be(true)
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

    retObj.futureValue should be(true)
  }
}