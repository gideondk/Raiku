package nl.gideondk.raiku

import akka.actor._
import commands.RWObject
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import spray.json._
import monads._

import scala.concurrent._
import scala.concurrent.duration._
import play.api.libs.iteratee.Iteratee

import scala.util.Random

trait ShortPerformanceTest extends PerformanceTest {
  println("***\n* Generating test items")

  val groupIds = List.fill(100)(Data.generateId)

  val groups = groupIds.map(x => WorkGroup(x, "WorkGroup"))

  val countries = List.fill(10)(Data.randomCountry)
  val cities = List.fill(100)(Data.randomCity(countries(Random.nextInt(10)).objectId))
  val streets = List.fill(500)(Data.randomStreet(cities(Random.nextInt(100)).objectId))
  val addresses = List.fill(300)(Data.randomAddress(streets(Random.nextInt(500)).objectId))
 
  val persons = List.fill(2000)(Data.randomPerson(Random.shuffle(groupIds).splitAt(5)._1, addresses(Random.nextInt(300))))

  val (firstHalf, secHalf) = persons.splitAt(persons.length / 2)

  val messageBoxes = List.fill(50) {
    val from = firstHalf(Random.nextInt(firstHalf.length)).email
    val to = secHalf(Random.nextInt(firstHalf.length)).email
    val messages = for (i <- 0 to 3050) yield Data.randomMessage
    MessageBox(Data.generateId, from, to, messages.toList)
  }

  def run: Unit = {
    storeGroupsSequentially
    storeGroupsParrallel
    storeEtc
    storageLargeDocFormat

    fetchSingleBucketParallel

    fetchCountriesSequential
    fetchCountriesParallel

    deleteCreatedItems
  }

  def storeGroupsSequentially = {
    timed("Storing " + groups.length + " groups sequentially into Riak (10 times)", groups.length * 10) {
      for (i <- 1 to 10) {
        val acts = groups.map(x => DB.groupBucket << x)
        acts.foreach { act =>
          act.unsafeFulFill(5 seconds)
        }
      }
    }
  }

  def storeGroupsParrallel = {
    val mulActs = for (i <- 1 to 10) yield ValidatedFutureIO.sequence(groups.map(x => DB.groupBucket << x))
    val comActs = ValidatedFutureIO.sequence(mulActs.toList)

    timed("Storing " + groups.length + " groups in parallel into Riak (10 times)", groups.length * 10) {
      val a = comActs.unsafeFulFill(15 seconds)
    }
  }

  def storeEtc = {
    val totalLength = countries.length + cities.length + streets.length + persons.length
    val ioActs = for (i <- 1 to 5) yield {
      for {
        _ <- DB.countryBucket <<* countries
        _ <- DB.cityBucket <<* cities
        _ <- DB.streetBucket <<* streets
        _ <- DB.personBucket <<* persons
      } yield ()
    }

    timed("Storing " + totalLength + " items in parallel into Riak (5 times)", totalLength * 5) {
      for (i <- 0 to 4) {
        val a = ioActs(i).unsafeFulFill(20 seconds)
      }
    }
  }

  def storageLargeDocFormat = {
    timed("Storing " + messageBoxes.length + " large items (~560k json) in parallel into Riak (20 times)", messageBoxes.length * 5) {
      for (i <- 1 to 5) {
        (DB.messageBoxBucket <<* messageBoxes).unsafeFulFill(20 seconds)
      }
    }
  }

  def fetchSingleBucketParallel {
    val personIds = persons.map(_.email)

    timed("Fetching " + personIds.length + " items in parallel from Riak (10 times)", personIds.length * 20) {
      for (i <- 1 to 20) {
        val act = DB.personBucket ?* personIds
        act.unsafeFulFill(5 seconds)
      }
    }
  }

  def fetchCountriesSequential {
    val personIds = persons.map(_.email)
    timed("Fetching " + personIds.length * 4 + " items in sequence from Riak (3 times)", personIds.length * 3) {
      for (i <- 1 to 3) {
        val acts = personIds.map(countryForPerson(_))
        acts.foreach { act =>
          act.unsafeFulFill(5 seconds)
        }
      }
    }
  }

  def fetchCountriesParallel {
    val personIds = persons.map(_.email)
    timed("Fetching " + personIds.length * 4 + " sequenced items in parallel from Riak (3 times)", personIds.length * 3) {
      for (i <- 1 to 3) {
        val act = ValidatedFutureIO.sequence(personIds.map(countryForPerson(_)))
        val res = act.unsafeFulFill(20 seconds)
      }
    }
  }

  def deleteCreatedItems = {
    val totalLength = countries.length + cities.length + streets.length + persons.length + groups.length + messageBoxes.length

    val delCountries = DB.countryBucket -* countries
    val delCities = DB.cityBucket -* cities
    val delStreets = DB.streetBucket -* streets
    val delPersons = DB.personBucket -* persons
    val delGroups = DB.groupBucket -* groups
    val delMbx = DB.messageBoxBucket -* messageBoxes

    val act = ValidatedFutureIO.sequence(List(delCountries, delCities, delStreets, delPersons, delGroups, delMbx))
    timed("Deleting " + totalLength + " items parallel from Riak", totalLength) {
      act.unsafeFulFill(60 seconds)
    }
  }

  /* Helper */
  def countryForPerson(personId: String) = {
    for {
      person <- DB.personBucket ? personId
      street <- DB.streetBucket ? person.get.address.streetId
      city <- DB.cityBucket ? street.get.cityId
      country <- DB.countryBucket ? city.get.countryId
    } yield country.get
  }

}
