package nl.gideondk.raiku

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

class RawSpec extends RaikuSpec {
  import TestModels._

  "A client" should {
    "be able to store rw objects into Riak" in {
      val newId = java.util.UUID.randomUUID.toString

      val rawObj = RaikuRawValue("raiku_test_bucket", newId, None, Some("text/plain"), None, None, Some("this should be stored".getBytes), None)
      val validation = client.storeRaw(rawObj, returnBody = Some(true))

      validation.futureValue.content.head.key should equal(rawObj.key)
    }

    "delete object properly" in {
      val newId = java.util.UUID.randomUUID.toString
      val rawObj = RaikuRawValue("raiku_test_bucket", newId, None, Some("text/plain"), None, None, Some("this should be stored".getBytes), None)

      val resp = for {
        _ ← client.storeRaw(rawObj)
        a ← client.fetchRaw(rawObj.bucket, rawObj.key)
        _ ← client.deleteRaw(rawObj)
        b ← client.fetchRaw(rawObj.bucket, rawObj.key)
      } yield {
        a.content.headOption.isDefined && b.content.headOption.isEmpty
      }

      resp.futureValue should equal(true)
    }
    "persist 2i properly" in {
      val newId = java.util.UUID.randomUUID.toString

      val orgIdA = java.util.UUID.randomUUID.toString
      val orgIdB = java.util.UUID.randomUUID.toString

      val indexes = RaikuIndexes(Map("organization_id" -> Set(orgIdA, orgIdB)), Map[String, Set[Int]]())
      val rawObj = RaikuRawValue("raiku_test_bucket", newId, None, Some("text/plain"), None, None, Some("this should be stored".getBytes), Some(RaikuMeta(indexes = indexes)))

      val validation = client.storeRaw(rawObj, returnBody = Some(true)).futureValue
      val retRawObject = client.fetchRaw("raiku_test_bucket", newId).map(_.content.head).futureValue

      indexes.binary.get("organization_id").get.toList.sortBy(x ⇒ x) should equal(retRawObject.meta.get.indexes.binary("organization_id").toList.sortBy(x ⇒ x))
    }
    "be able to retrieve object by 2i" in {
      val newId = java.util.UUID.randomUUID.toString
      val anotherId = java.util.UUID.randomUUID.toString

      val orgIdA = java.util.UUID.randomUUID.toString
      val orgIdB = java.util.UUID.randomUUID.toString
      val orgIdC = java.util.UUID.randomUUID.toString

      val indexesA = RaikuIndexes(Map("organization_id" -> Set(orgIdA, orgIdB)), Map[String, Set[Int]]())
      val rawObjA = RaikuRawValue("raiku_test_bucket", newId, None, Some("text/plain"), None, None, Some("this should be stored".getBytes), Some(RaikuMeta(indexes = indexesA)))

      val indexesB = RaikuIndexes(Map("organization_id" -> Set(orgIdB, orgIdC)), Map[String, Set[Int]]())
      val rawObjB = RaikuRawValue("raiku_test_bucket", anotherId, None, Some("text/plain"), None, None, Some("this should be stored".getBytes), Some(RaikuMeta(indexes = indexesB)))

      val keys = for {
        _ ← client.storeRaw(rawObjA)
        _ ← client.storeRaw(rawObjB)
        aKeys ← client.fetchKeysForBinIndexByValue("raiku_test_bucket", "organization_id", orgIdA)
        cKeys ← client.fetchKeysForBinIndexByValue("raiku_test_bucket", "organization_id", orgIdC)
      } yield (aKeys, cKeys)

      keys.futureValue should equal(List(newId), List(anotherId))
    }
  }
}