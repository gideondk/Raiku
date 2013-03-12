package nl.gideondk.raiku

import akka.actor._
import commands.RWObject
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import spray.json._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

import scala.concurrent._
import scala.concurrent.duration._
import play.api.libs.iteratee.Iteratee

case class Country(objectId: String, name: String)
case class City(objectId: String, name: String, countryId: String)
case class Street(objectId: String, name: String, cityId: String)
case class Address(objectId: String, streetId: String, streetNumber: Int)

case class Person(email: String, firstName: String, lastName: String, age: Int, address: Address, groupIds: List[String])

case class WorkGroup(objectId: String, name: String)

case class Message(objectId: String, text: String, created: Int)

case class MessageBox(objectId: String, from: String, to: String, messages: List[Message])

case class WorkGroupMessage(objectId: String, text: String, fromPerson: String, to: String, created: Int)

object TestSerialization extends DefaultJsonProtocol {
  implicit val countryFormat = jsonFormat2(Country)
  implicit val cityFormat = jsonFormat3(City)
  implicit val streetFormat = jsonFormat3(Street)
  implicit val addressFormat = jsonFormat3(Address)

  implicit val personFormat = jsonFormat6(Person)
  implicit val workGroupFormat = jsonFormat2(WorkGroup)

  implicit val messageFormat = jsonFormat3(Message)
  implicit val messageBoxFormat = jsonFormat4(MessageBox)

  implicit val workGroupMessageFormat = jsonFormat5(WorkGroupMessage)

  implicit val countryConverter = new RaikuConverter[Country] {
    def read(o: RWObject): ReadResult[Country] = try {
      countryFormat.read(new String(o.value).asJson).success
    } catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: Country): RWObject = RWObject(bucket, o.objectId, o.toJson.toString.getBytes)
  }

  implicit val cityConverter = new RaikuConverter[City] {
    def read(o: RWObject): ReadResult[City] = try {
      cityFormat.read(new String(o.value).asJson).success
    } catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: City): RWObject = RWObject(bucket, o.objectId, o.toJson.toString.getBytes,
      binIndexes = Map("country_id" -> List(o.countryId)))
  }

  implicit val streetConverter = new RaikuConverter[Street] {
    def read(o: RWObject): ReadResult[Street] = try {
      streetFormat.read(new String(o.value).asJson).success
    } catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: Street): RWObject = RWObject(bucket, o.objectId, o.toJson.toString.getBytes,
      binIndexes = Map("city_id" -> List(o.cityId)))
  }

  implicit val personConverter = new RaikuConverter[Person] {
    def read(o: RWObject): ReadResult[Person] = try {
      personFormat.read(new String(o.value).asJson).success
    } catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: Person): RWObject = RWObject(bucket, o.email, o.toJson.toString.getBytes,
      binIndexes = Map("group_id" -> o.groupIds), intIndexes = Map("age" -> List(o.age)))
  }

  implicit val workGroupConverter = new RaikuConverter[WorkGroup] {
    def read(o: RWObject): ReadResult[WorkGroup] = try {
      workGroupFormat.read(new String(o.value).asJson).success
    } catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: WorkGroup): RWObject = RWObject(bucket, o.objectId, o.toJson.toString.getBytes)
  }

  // implicit val messageConverter = new RaikuConverter[Message] {
  //   def read(o: RWObject): ReadResult[Message] = try {
  //     messageFormat.read(new String(o.value).asJson).success
  //   }
  //   catch {
  //     case e: Throwable ⇒ e.failure
  //   }
  //   def write(bucket: String, o: Message): RWObject = RWObject(bucket, o.objectId, o.toJson.toString.getBytes,
  //   	intIndexes = Map("created" -> List(o.created)))
  // }

  implicit val messageBoxConverter = new RaikuConverter[MessageBox] {
    def read(o: RWObject): ReadResult[MessageBox] = try {
      messageBoxFormat.read(new String(o.value).asJson).success
    } catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: MessageBox): RWObject = RWObject(bucket, o.objectId, o.toJson.toString.getBytes,
      binIndexes = Map("from" -> List(o.from), "to" -> List(o.to)))
  }

  implicit val workGroupMessageConverter = new RaikuConverter[WorkGroupMessage] {
    def read(o: RWObject): ReadResult[WorkGroupMessage] = try {
      workGroupMessageFormat.read(new String(o.value).asJson).success
    } catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: WorkGroupMessage): RWObject = RWObject(bucket, o.objectId, o.toJson.toString.getBytes,
      binIndexes = Map("from_person" -> List(o.fromPerson), "to" -> List(o.to)), intIndexes = Map("created" -> List(o.created)))
  }
}

object DB {
  import TestSerialization._

  implicit val system = ActorSystem("perf-bucket-system")
  val client = RaikuClient("localhost", 8087, 4)

  val groupBucket = RaikuBucket[WorkGroup]("raiku_perf_test_group_bucket", client)

  val countryBucket = RaikuBucket[Country]("raiku_perf_test_country_bucket", client)
  val cityBucket = RaikuBucket[City]("raiku_perf_test_city_bucket", client)
  val streetBucket = RaikuBucket[Street]("raiku_perf_test_street_bucket", client)

  val personBucket = RaikuBucket[Person]("raiku_perf_test_person_bucket", client)

  val messageBoxBucket = RaikuBucket[MessageBox]("raiku_perf_test_message_box_bucket", client)

  val reactiveGroupBucket = RaikuReactiveBucket[WorkGroup]("raiku_perf_test_group_bucket", client)
  val reactiveMessageBoxBucket = RaikuReactiveBucket[MessageBox]("raiku_perf_test_message_box_bucket", client)

  def workGroupMessageBucket(groupId: String) = RaikuBucket[WorkGroupMessage]("raiku_perf_test_workgroup_" + groupId + "_message_bucket", client)
}
