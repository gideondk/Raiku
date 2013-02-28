package nl.gideondk.raiku

import scala.util.Random

import scalaz._
import Scalaz._
import java.util.Date

import spray.json._

object Data {
	val firstNames = scala.io.Source.fromFile("src/benchmark/scala/nl/gideondk/raiku/json/first_names.json").mkString.asJson.asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsString].value)
	val lastNames = scala.io.Source.fromFile("src/benchmark/scala/nl/gideondk/raiku/json/last_names.json").mkString.asJson.asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsString].value)

	val places = scala.io.Source.fromFile("src/benchmark/scala/nl/gideondk/raiku/json/places.json").mkString.asJson.asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsString].value)

	val firstNamesCount = firstNames.length
	val lastNamesCount = lastNames.length
	val placesCount = places.length

	def randomName = (firstNames(Random.nextInt(firstNamesCount)), lastNames(Random.nextInt(lastNamesCount)))

	def randomNames(count: Int) = {
		
		val names = for (i <- 0 to count) yield {
			(firstNames(Random.nextInt(firstNamesCount)), lastNames(Random.nextInt(lastNamesCount)))
		}
		names.toList
	}

	def randomPerson(groupIds: List[String], address: Address) = {
		val name = randomName
		val email = name._1.toLowerCase + "." + name._2.toLowerCase + "@domain.com"
		val age = Random.nextInt(99)
		Person(email, name._1, name._2, age, address, groupIds)
	}

	def randomCountry = Country(generateId, places(Random.nextInt(placesCount)))

	def randomCity(countryId: String) = City(generateId, places(Random.nextInt(placesCount)), countryId)

	def randomStreet(cityId: String) = Street(generateId, "not-random-yet", cityId)

	def randomAddress(streetId: String) = Address(generateId, streetId, Random.nextInt(240))

	def randomMessage = Message(generateId, """You little ripper pav where built like a wobbly. You little ripper aussie salute no dramas come a scratchy. Built like a captain cook my lets get some thingo. Get a dog up ya nipper no dramas we're going wuss. Lets get some boogie board where as cross as a bush oyster. As stands out like mappa tassie no dramas he's got a massive roo. Gutful of boozer with lets throw a stubby. wuss no dramas you little ripper brickie. As cunning as a rip snorter where lets get some beauty.
As dry as a postie to it'll be cactus mate. Shazza got us some rotten flamin mad as a cactus mate. He's got a massive rego with she'll be right chrissie. Stands out like a damper to as dry as a show pony. It'll be strewth bloody as cross as a show pony. We're going ripper with she'll be right throw-down.
It'll be gobsmacked flamin as dry as a down under. As cunning as a shag on a rock my get a dog up ya too right!. As cunning as a cut snake piece of piss get a dog up ya plonk. Gutful of bradman where flat out like a bluey. Built like a milk bar with as busy as a lizard drinking. As busy as a slabs my you little ripper cactus mate. spit the dummy to stands out like a daks. She'll be right feral also he's got a massive piker.
Lets throw a hooroo heaps shonky. Come a bonzer mate she'll be right bloke. Flat out like a banana bender flamin as dry as a dickhead. It'll be pav how as dry as a strides. Trent from punchy strides to gutful of mullet. sook bloody lets get some feral. Grab us a daks bloody mad as a lurk. She'll be right bastard heaps as cunning as a roo bar.""", ((new Date).getTime / 1000).toInt)

	def generateId = java.util.UUID.randomUUID.toString
}