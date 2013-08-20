package nl.gideondk.raiku

import akka.actor._

import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import spray.json._

import org.specs2.mutable._
import play.api.libs.iteratee._

import scala.util.{ Success, Failure }

class BucketAdvancedSpec extends RaikuSpec {
  import TestModels._

  val bucket = RaikuBucket[Y]("raiku_test_y_bucket_"+java.util.UUID.randomUUID.toString, client)
  bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))).copoint

  "A bucket" should {
    "be able to retrieve objects with their binary 2i" in {
      val newId = java.util.UUID.randomUUID.toString
      val groupId = java.util.UUID.randomUUID.toString
      val obj = Y(newId, "Matsuo Bashō", 41, groupId)

      val key = for {
        _ ← bucket << obj
        idxf ← bucket idx ("group_id", groupId)
      } yield idxf

      val res = key.copoint
      res must have length 1
      res.head must beEqualTo(newId)
    }

    "be able to retrieve objects with their integeral 2i" in {
      val newId = java.util.UUID.randomUUID.toString
      val groupId = java.util.UUID.randomUUID.toString
      val obj = Y(newId, "Matsuo Bashō", 41, groupId)

      val keys = for {
        _ ← bucket << obj
        idxf ← bucket idx ("age", 41)
      } yield idxf

      val res = keys.copoint

      res must contain(newId)
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

      val res = keys.copoint

      res._1 must contain(newId)
      res._1 must contain(secId)

      res._2 must contain(newId)
      res._2 must not contain (secId)

      res._3 must not contain (newId)
      res._3 must contain(secId)
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

      val res = keys.copoint

      res._1 must contain(newId)
      res._1 must contain(secId)
      res._1 must contain(thirdId)

      res._2 must contain(newId)
      res._2 must contain(secId)
      res._2 must not contain (thirdId)

      res._3 must not contain (newId)
      res._3 must contain(secId)
      res._3 must contain(thirdId)
    }

    "be able to retrieve objects with a maximum number of results" in {
      val groupId = java.util.UUID.randomUUID.toString
      val objs = List.fill(50)(Y(java.util.UUID.randomUUID.toString, "Matsuo Bashō", 41, groupId))

      val i = for {
        _ ← bucket <<* objs
        items ← bucket idx ("age", 40 to 41, 25)
      } yield items

      val res = i.copoint
      res._2 must have length 25
    }

    "be able to paginate index results" in {
      val groupId = java.util.UUID.randomUUID.toString
      val objs = List.fill(200)(Y(java.util.UUID.randomUUID.toString, "Matsuo Bashō", scala.util.Random.nextInt(99), groupId))

      val i = for {
        _ ← bucket <<* (objs, r = 3, w = 3)
        f ← bucket idx ("group_id", groupId, 4)
        s ← bucket idx ("group_id", groupId, 4, f._1)
        t ← bucket idx ("group_id", groupId, 4, s._1)
        l ← bucket idx ("group_id", groupId, 4, t._1)
      } yield f._2 ++ s._2 ++ t._2 ++ l._2

      val res = i.copoint
      res must have length 16
    }

    "be able to paginate ranged index results" in {
      val groupId = java.util.UUID.randomUUID.toString
      val objs = List.fill(200)(Y(java.util.UUID.randomUUID.toString, "Matsuo Bashō", scala.util.Random.nextInt(99), groupId))

      val i = for {
        _ ← bucket <<* (objs, r = 3, w = 3)
        f ← bucket idx ("age", 20 to 80, 4)
        s ← bucket idx ("age", 20 to 80, 4, f._1)
        t ← bucket idx ("age", 20 to 80, 4, s._1)
        l ← bucket idx ("age", 20 to 80, 4, t._1)
      } yield f._2 ++ s._2 ++ t._2 ++ l._2

      val res = i.copoint
      res must have length 16
    }

    "be able to stream index results" in {
      val idxs = (bucket streamIdx ("age", 25)).flatMap(x ⇒ Task(x |>>> Iteratee.getChunks))
      val normalQuery = bucket idx ("age", 25)

      idxs.copoint must have length normalQuery.copoint.length
    }

    "a counter should increment correctly" in {
      val counter = bucket counter "like_count"
      val tpl = for {
        initial ← counter.get
        firstAdd ← counter += 5
        secAdd ← counter += 20
        thirdAdd ← counter += 70
        _ ← counter -= 40
        lastValue ← counter.get
      } yield (initial, thirdAdd, lastValue)

      val res = tpl.copoint
      res._1 must beEqualTo(0)
      res._2 must beEqualTo(95)
      res._3 must beEqualTo(55)
    }

    "be able to use Scalaz functionality on Tasks" in {
      val newId = java.util.UUID.randomUUID.toString
      val secId = java.util.UUID.randomUUID.toString
      val groupId = java.util.UUID.randomUUID.toString

      val basho = Y(newId, "Matsuo Bashō", 41, groupId)
      val shiki = Y(secId, "Masaoka Shiki", 52, groupId)

      val all = for {
        _ ← bucket << basho
        _ ← bucket << shiki
        all ← bucket idx ("age", 40 to 60) >>= ((x: List[String]) ⇒ bucket ?* x)
      } yield (all)

      val res = all.run
      res.isSuccess must beTrue
    }
  }

}
