package nl.gideondk.raiku

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

class BucketAdvancedSpec extends RaikuSpec {
  import TestModels._

  val bucket = RaikuBucket.default[Y]("raiku_test_y_bucket_"+java.util.UUID.randomUUID.toString, client)
  val typedBucketA = RaikuBucket.typed[Y]("type_a", bucket.bucketName, client)
  val typedBucketB = RaikuBucket.typed[Y]("type_b", bucket.bucketName, client)

  Await.result(bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))), 5 seconds)

  "A bucket" should {
    "be able to retrieve objects with their binary 2i" in {
      val newId = java.util.UUID.randomUUID.toString
      val groupId = java.util.UUID.randomUUID.toString
      val obj = Y(newId, "Matsuo Bashō", 41, groupId)

      val key = for {
        _ ← bucket << obj
        idxf ← bucket idx ("group_id", groupId)
      } yield idxf

      key.futureValue should contain(newId)
    }

    "be able to retrieve objects with their integeral 2i" in {
      val newId = java.util.UUID.randomUUID.toString
      val groupId = java.util.UUID.randomUUID.toString
      val obj = Y(newId, "Matsuo Bashō", 41, groupId)

      val keys = for {
        _ ← bucket << obj
        idxf ← bucket idx ("age", 41)
      } yield idxf

      keys.futureValue should contain(newId)
    }

    "be able to retrieve objects with ranges on a integeral 2i" in {
      val newId = java.util.UUID.randomUUID.toString
      val secId = java.util.UUID.randomUUID.toString
      val groupId = java.util.UUID.randomUUID.toString

      val basho = Y(newId, "Matsuo Bashō", 41, groupId)
      val shiki = Y(secId, "Masaoka Shiki", 52, groupId)

      val keys = for {
        _ ← bucket << basho
        _ ← bucket << shiki
        all ← bucket idx ("age", 40 to 60)
        basho ← bucket idx ("age", 39 to 42)
        shiki ← bucket idx ("age", 50 to 60)
      } yield (all, basho, shiki)

      keys.futureValue._1 should contain(newId)
      keys.futureValue._1 should contain(secId)

      keys.futureValue._2 should contain(newId)
      keys.futureValue._2 shouldNot contain(secId)

      keys.futureValue._3 shouldNot contain(newId)
      keys.futureValue._3 should contain(secId)
    }
  }

  "be able to retrieve objects with ranges on a binary 2i" in {
    val newId = java.util.UUID.randomUUID.toString
    val secId = java.util.UUID.randomUUID.toString
    val thirdId = java.util.UUID.randomUUID.toString

    val persA = Y(newId, "Person A", 41, "A")
    val persB = Y(secId, "Person B", 52, "B")
    val persC = Y(thirdId, "Person C", 12, "C")

    val keys = for {
      _ ← bucket << persA
      _ ← bucket << persB
      _ ← bucket << persC
      all ← bucket idx ("group_id", "A" to "C")
      aAndB ← bucket idx ("group_id", "A" to "B")
      bAndC ← bucket idx ("group_id", "B" to "C")
    } yield (all, aAndB, bAndC)

    keys.futureValue._1 should contain(newId)
    keys.futureValue._1 should contain(secId)
    keys.futureValue._1 should contain(thirdId)

    keys.futureValue._2 should contain(newId)
    keys.futureValue._2 should contain(secId)
    keys.futureValue._2 shouldNot contain(thirdId)

    keys.futureValue._3 shouldNot contain(newId)
    keys.futureValue._3 should contain(secId)
    keys.futureValue._3 should contain(thirdId)
  }

  "be able to retrieve objects with a maximum number of results" in {
    val groupId = java.util.UUID.randomUUID.toString
    val objs = List.fill(50)(Y(java.util.UUID.randomUUID.toString, "Matsuo Bashō", 41, groupId))

    val i = for {
      _ ← bucket <<* objs
      items ← bucket idx ("age", 40 to 41, 25)
    } yield items

    i.futureValue._2.length should equal(25)
  }

  "be able to paginate index results" in {
    val groupId = java.util.UUID.randomUUID.toString
    val objs = List.fill(200)(Y(java.util.UUID.randomUUID.toString, "Matsuo Bashō", scala.util.Random.nextInt(99), groupId))

    val i = for {
      _ ← bucket <<* objs
      f ← bucket idx ("group_id", groupId, 4)
      s ← bucket idx ("group_id", groupId, 4, f._1)
      t ← bucket idx ("group_id", groupId, 4, s._1)
      l ← bucket idx ("group_id", groupId, 4, t._1)
    } yield f._2 ++ s._2 ++ t._2 ++ l._2

    i.futureValue.length should equal(16)
  }

  "be able to paginate ranged index results" in {
    val groupId = java.util.UUID.randomUUID.toString
    val objs = List.fill(200)(Y(java.util.UUID.randomUUID.toString, "Matsuo Bashō", scala.util.Random.nextInt(99), groupId))

    val i = for {
      _ ← bucket <<* objs
      f ← bucket idx ("age", 20 to 80, 4)
      s ← bucket idx ("age", 20 to 80, 4, f._1)
      t ← bucket idx ("age", 20 to 80, 4, s._1)
      l ← bucket idx ("age", 20 to 80, 4, t._1)
    } yield f._2 ++ s._2 ++ t._2 ++ l._2

    i.futureValue.length should equal(16)
  }

  "be able to stream index results" in {
    implicit val system = DB.system
    implicit val mat = ActorMaterializer()

    val idxs = (bucket streamIdx ("age", 25)).flatMap(_.runWith(Sink.seq))
    val normalQuery = bucket idx ("age", 25)

    idxs.futureValue.length should equal(normalQuery.futureValue.length)
  }

  /*
  *
  * Will currently only run with manual intervention, e.g.
   *
  * riak-admin bucket-type create type_a
  * riak-admin bucket-type create type_b
  *
  * riak-admin bucket-type activate type_a
  * riak-admin bucket-type activate type_b
  *
  * */

  "handle bucket types correctly" in {
    val objA = Y("K", "1", 0, "A")
    val objB = Y("K", "2", 1, "A")
    val objC = Y("K", "3", 2, "A")

    val comparison = for {
      _ ← bucket << objA
      _ ← typedBucketA << objB
      _ ← typedBucketB << objC
      v1 ← bucket ? "K"
      v2 ← typedBucketA ? "K"
      v3 ← typedBucketB ? "K"
      k1 ← bucket idx ("group_id", "A")
      k2 ← typedBucketA idx ("group_id", "A")
      k3 ← typedBucketB idx ("group_id", "A")
    } yield {
      v1.get.name == "1" && v2.get.name == "2" && v3.get.name == "3" && k1.length == 1 && k2.length == 1 && k3.length == 1
    }

    comparison.futureValue
  }

}
