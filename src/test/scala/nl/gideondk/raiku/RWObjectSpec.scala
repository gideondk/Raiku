package nl.gideondk.raiku

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter
import akka.actor._
import commands.RaikuRWObject
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

class RWObjectSpec extends Specification {

  implicit val system = ActorSystem("rw-system")
  val client = RaikuClient("localhost", 8087, 4)

  "A client" should {
    "be able to store rw objects into Riak" in {
      val newId = java.util.UUID.randomUUID.toString

      val rwObject = RaikuRWObject("raiku_test_bucket", newId, "this should be stored".getBytes)
      val validation: Validation[Throwable, List[RaikuRWObject]] = client.store(rwObject).unsafeFulFill()
      validation.isSuccess
    }
    "return stored items properly" in {
      val newId = java.util.UUID.randomUUID.toString
      val rwObject = RaikuRWObject("raiku_test_bucket", newId, "this should be stored".getBytes)
      val validation = client.store(rwObject).unsafeFulFill

      val retRWObject = client.fetch("raiku_test_bucket", newId).unsafeFulFill

      validation.isSuccess && retRWObject.isSuccess && retRWObject.toOption.get.headOption.isDefined && new String(retRWObject.toOption.get.head.value) == new String(rwObject.value)
    }
    "delete object properly" in {
      val newId = java.util.UUID.randomUUID.toString
      val rwObject = RaikuRWObject("raiku_test_bucket", newId, "this should be stored".getBytes)
      val validation = client.store(rwObject).unsafeFulFill

      client.delete(rwObject).unsafeFulFill

      val retRWObject = client.fetch("raiku_test_bucket", newId).unsafeFulFill

      validation.isSuccess && retRWObject.isSuccess && !retRWObject.toOption.get.headOption.isDefined
    }
    "persist 2i properly" in {
      val newId = java.util.UUID.randomUUID.toString

      val orgIdA = java.util.UUID.randomUUID.toString
      val orgIdB = java.util.UUID.randomUUID.toString

      val rwObject = RaikuRWObject("raiku_test_bucket", newId, "this should be stored".getBytes, binIndexes = Map("organization_id" -> List(orgIdA, orgIdB)))
      val validation = client.store(rwObject).unsafeFulFill()
      val retRWObject = client.fetch("raiku_test_bucket", newId).unsafeFulFill.toOption.get.head

      rwObject.binIndexes.get("organization_id").get.sortBy(x ⇒ x) == retRWObject.binIndexes.get("organization_id").get.sortBy(x ⇒ x)
    }
    "be able to retrieve object by 2i" in {
      val newId = java.util.UUID.randomUUID.toString
      val anotherId = java.util.UUID.randomUUID.toString

      val orgIdA = java.util.UUID.randomUUID.toString
      val orgIdB = java.util.UUID.randomUUID.toString
      val orgIdC = java.util.UUID.randomUUID.toString

      val rwObjectA = RaikuRWObject("raiku_test_bucket", newId, "this should be stored".getBytes, binIndexes = Map("organization_id" -> List(orgIdA, orgIdB)))
      val rwObjectB = RaikuRWObject("raiku_test_bucket", anotherId, "this should be stored".getBytes, binIndexes = Map("organization_id" -> List(orgIdB, orgIdC)))

      val store = for {
        _ ← client.store(rwObjectA)
        _ ← client.store(rwObjectB)
      } yield ()

      store.unsafeFulFill

      val keysIO = for {
        aKeys ← client.fetchKeysForBinIndexByValue("raiku_test_bucket", "organization_id", orgIdA)
        cKeys ← client.fetchKeysForBinIndexByValue("raiku_test_bucket", "organization_id", orgIdC)
      } yield (aKeys, cKeys)

      val keys = keysIO.unsafeFulFill
      keys.toOption.get == (List(newId), List(anotherId))
    }
  }

  step {
    client.disconnect
    system.shutdown()
  }
}