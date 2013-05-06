package nl.gideondk.raiku

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter
import akka.actor._
//import commands.RWObject
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

class RWObjectSpec extends Specification {
  sequential

  implicit val timeout = Duration(5, duration.SECONDS)
  val client = DB.client

  "A client" should {
    "be able to store rw objects into Riak" in {
      val newId = java.util.UUID.randomUUID.toString

      val rawObj = RaikuRawValue("raiku_test_bucket", newId, Some("text/plain"), None, None, "this should be stored".getBytes.point[Option], None)
      // val rwObject = RWObject("raiku_test_bucket", newId, "this should be stored".getBytes.point[Option])
      val validation = client.store(rawObj).run
      validation.isSuccess
    }
    "return stored items properly" in {
      val newId = java.util.UUID.randomUUID.toString
      val rawObj = RaikuRawValue("raiku_test_bucket", newId, Some("text/plain"), None, None, "this should be stored".getBytes.point[Option], None)
      val validation = client.store(rawObj).run

      val retRWObject = client.fetch("raiku_test_bucket", newId).run

      validation.isSuccess && retRWObject.isSuccess && retRWObject.toOption.get.content.headOption.isDefined && new String(retRWObject.toOption.get.content.head.value.get) == new String(rawObj.value.get)
    }
    "delete object properly" in {
      val newId = java.util.UUID.randomUUID.toString
      val rawObj = RaikuRawValue("raiku_test_bucket", newId, Some("text/plain"), None, None, "this should be stored".getBytes.point[Option], None)
      val validation = client.store(rawObj).run

      client.delete(rawObj).run

      val retRawObj = client.fetch("raiku_test_bucket", newId).run

      validation.isSuccess && retRawObj.isSuccess && !retRawObj.toOption.get.content.headOption.isDefined
    }
    "persist 2i properly" in {
      val newId = java.util.UUID.randomUUID.toString

      val orgIdA = java.util.UUID.randomUUID.toString
      val orgIdB = java.util.UUID.randomUUID.toString

      val indexes = RaikuIndexes(Map("organization_id" -> Set(orgIdA, orgIdB)), Map[String, Set[Int]]())
      val rawObj = RaikuRawValue("raiku_test_bucket", newId, Some("text/plain"), None, None, "this should be stored".getBytes.point[Option], Some(RaikuMeta(indexes = indexes)))

      val validation = client.store(rawObj, returnBody = Option(true)).run
      val retRawObject = client.fetch("raiku_test_bucket", newId).run.toOption.get.content.head

      indexes.binary.get("organization_id").get.toList.sortBy(x ⇒ x) == retRawObject.meta.get.indexes.binary("organization_id").toList.sortBy(x ⇒ x)
    }
    "be able to retrieve object by 2i" in {
      val newId = java.util.UUID.randomUUID.toString
      val anotherId = java.util.UUID.randomUUID.toString

      val orgIdA = java.util.UUID.randomUUID.toString
      val orgIdB = java.util.UUID.randomUUID.toString
      val orgIdC = java.util.UUID.randomUUID.toString

      val indexesA = RaikuIndexes(Map("organization_id" -> Set(orgIdA, orgIdB)), Map[String, Set[Int]]())
      val rawObjA = RaikuRawValue("raiku_test_bucket", newId, Some("text/plain"), None, None, "this should be stored".getBytes.point[Option], Some(RaikuMeta(indexes = indexesA)))

      val indexesB = RaikuIndexes(Map("organization_id" -> Set(orgIdB, orgIdC)), Map[String, Set[Int]]())
      val rawObjB = RaikuRawValue("raiku_test_bucket", anotherId, Some("text/plain"), None, None, "this should be stored".getBytes.point[Option], Some(RaikuMeta(indexes = indexesB)))

      val store = for {
        _ ← client.store(rawObjA)
        _ ← client.store(rawObjB)
      } yield ()

      store.copoint

      val keysIO = for {
        aKeys ← client.fetchKeysForBinIndexByValue("raiku_test_bucket", "organization_id", orgIdA)
        cKeys ← client.fetchKeysForBinIndexByValue("raiku_test_bucket", "organization_id", orgIdC)
      } yield (aKeys, cKeys)

      val keys = keysIO.run
      keys.toOption.get == (List(newId), List(anotherId))
    }
  }
}