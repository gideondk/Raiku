package nl.gideondk.raiku

import akka.actor._

import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import spray.json._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

import scala.util.{ Success, Failure }

class BucketAdvancedSpec extends RaikuSpec {
  import TestModels._

  val bucket = RaikuBucket[Y]("raiku_test_y_bucket", client)
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
      assert(res.length == 1 && res.head == newId)
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
      assert(res.contains(newId))
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
      res._1.contains(newId) && res._1.contains(secId) && res._2.contains(newId) && !res._2.contains(secId) && !res._3.contains(newId) && res._3.contains(secId)
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

      val res = all.copoint
      true
    }
  }

}