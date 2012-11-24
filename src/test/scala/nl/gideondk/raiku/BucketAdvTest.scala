package nl.gideondk.raiku

import akka.actor._
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import scalaz._
import Scalaz._

import spray.json._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher


case class Y(id: String, name: String, age: Int, groupId: String)

class BucketAdvancedSpec extends Specification with DefaultJsonProtocol {

	implicit val system = ActorSystem("adv-bucket-system")
	val client = RaikuClient("localhost", 8087, 4)

	implicit val yFormat = jsonFormat4(Y)

  	implicit val yConverter = new RaikuConverter[Y] {
  		def read(o: RaikuRWObject): ReadResult[Y] = try {
  			yFormat.read(new String(o.value).asJson).success
  		} catch {
  			case e : Throwable => e.failure
  		}
  		def write(bucket: String, o: Y): RaikuRWObject = RaikuRWObject(bucket, o.id, o.toJson.toString.getBytes, 
  			binIndexes = Map("group_id" -> List(o.groupId)), intIndexes = Map("age" -> List(o.age)))
  	}
  	
	val bucket = RaikuBucket[Y]("raiku_test_y_bucket", client)
	bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))).unsafeFulFill

	

	"A bucket" should {
		"be able to retrieve objects with their binary 2i" in {
			val newId = java.util.UUID.randomUUID.toString
			val groupId = java.util.UUID.randomUUID.toString
			val obj = Y(newId, "Matsuo Bashō", 41, groupId)

			val key = for {
				_ 		<- bucket << obj
				idxf 	<- bucket idx ("group_id", groupId)
			} yield idxf

			val res = key.unsafeFulFill
			assert(res.isSuccess && res.toOption.get.length == 1 && res.toOption.get.head == newId)
		}
		"be able to retrieve objects with their integeral 2i" in {
			val newId = java.util.UUID.randomUUID.toString
			val groupId = java.util.UUID.randomUUID.toString
			val obj = Y(newId, "Matsuo Bashō", 41, groupId)

			val keys = for {
				_ 		<- bucket << obj
				idxf 	<- bucket idx ("age", 41)
			} yield idxf

			val res = keys.unsafeFulFill
			res.toOption.get.contains(newId)
		}
		"be able to retrieve objects with ranges on a integeral 2i" in {
			val newId = java.util.UUID.randomUUID.toString
			val secId = java.util.UUID.randomUUID.toString
			val groupId = java.util.UUID.randomUUID.toString
			
			val basho = Y(newId, "Matsuo Bashō", 41, groupId)
			val shiki = Y(secId, "Masaoka Shiki", 52, groupId)

			val keys = for {
				_ 		<- bucket << basho
				_ 		<- bucket << shiki
				all 	<- bucket idx ("age", 40 to 60)
				basho 	<- bucket idx ("age", 39 to 42)
				shiki 	<- bucket idx ("age", 50 to 60)
			} yield (all, basho, shiki)

			val res = keys.unsafeFulFill.toOption.get
			res._1.contains(newId) && res._1.contains(secId) && res._2.contains(newId) && !res._2.contains(secId) && !res._3.contains(newId) && res._3.contains(secId)
		}
	}

	step { 
		client.disconnect
		system.shutdown() 
	}
}


// class BucketAdvTest extends FunSuite with BeforeAndAfter with DefaultJsonProtocol {
// 	implicit val yFormat = jsonFormat4(Y)
//   	implicit val system = ActorSystem("system")

//   	implicit val yConverter = new RaikuConverter[Y] {
//   		def read(o: RaikuRWObject): ReadResult[Y] = try {
//   			yFormat.read(new String(o.value).asJson).success
//   		} catch {
//   			case e : Throwable => e.failure
//   		}
//   		def write(bucket: String, o: Y): RaikuRWObject = RaikuRWObject(bucket, o.id, o.toJson.toString.getBytes, 
//   			binIndexes = Map("group_id" -> List(o.groupId)), intIndexes = Map("age" -> List(o.age)))
//   	}

//   	var bucket: RaikuBucket[Y] = _
// 	var client:RaikuClient = _

// 	before {
// 		client = RaikuClient("localhost", 8087, 4)
// 		bucket = RaikuBucket[Y]("y_bucket", client)
// 		bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))).unsafeFulFill
// 	}

// 	after {
// 		client disconnect 
// 	}
	
// 	test("Objects should be retrieved from bucket when using 2i") {
// 		val newId = java.util.UUID.randomUUID.toString
// 		val groupId = java.util.UUID.randomUUID.toString
// 		val obj = Y(newId, "Matsuo Bashō", 41, groupId)

// 		val key = for {
// 			_ 		<- bucket << obj
// 			idxf 	<- bucket idx ("group_id", groupId)
// 		} yield idxf

// 		val res = key.unsafeFulFill
// 		assert(res.isSuccess && res.toOption.get.length == 1 && res.toOption.get.head == newId)
// 	}
// 	// test("Client should be able to store objects into buckets") {
// 	// 	val newId = java.util.UUID.randomUUID.toString
// 	// 	val obj = Z(newId, "Should also be stored")
// 	// 	val v =  bucket << obj
// 	// 	assert(v.unsafeFulFill.isSuccess)	
// 	// }

// 	// test("Stored objects in buckets should persist correctly") {
// 	// 	val newId = java.util.UUID.randomUUID.toString
// 	// 	val obj = Z(newId, "Should also be stored")

// 	// 	val retObj = for {
// 	// 		v 		<- bucket unsafeStoreNew obj
// 	// 		retObj	<- bucket ? obj.id
// 	// 	} yield {
// 	// 		retObj
// 	// 	} 

// 	// 	assert(retObj.unsafeFulFill.toOption.get.get == obj)	
// 	// }

// 	// test("Unsafe updating objects should create siblings (and fail)") {
// 	// 	val newId = java.util.UUID.randomUUID.toString
// 	// 	val obj = Z(newId, "Should also be stored")

// 	// 	val retObj = for {
// 	// 		v 		<- bucket unsafeStoreNew obj
// 	// 		retObj	<- bucket ? obj.id
// 	// 	} yield {
// 	// 		retObj
// 	// 	} 

// 	// 	retObj.unsafeFulFill

// 	// 	val updatedObj = for {
// 	// 		v 	<- bucket unsafeStoreNew obj
// 	// 		retObj	<- bucket ? obj.id
// 	// 	} yield {
// 	// 		retObj
// 	// 	} 

// 	// 	val validation = updatedObj.unsafeFulFill
// 	// 	assert(validation.isFailure)	
// 	// }

// 	// test("Safely updating objects shouldn't create siblings") {
// 	// 	val newId = java.util.UUID.randomUUID.toString
// 	// 	val obj = Z(newId, "Should also be stored")

// 	// 	val retObj = for {
// 	// 		v 		<- bucket << obj
// 	// 		retObj	<- bucket ? obj.id
// 	// 	} yield {
// 	// 		retObj
// 	// 	} 

// 	// 	retObj.unsafeFulFill

// 	// 	val updatedObj = for {
// 	// 		v 		<- bucket << obj
// 	// 		retObj	<- bucket ? obj.id
// 	// 	} yield {
// 	// 		retObj
// 	// 	} 

// 	// 	val validation = updatedObj.unsafeFulFill
// 	// 	assert(validation.isSuccess)	
// 	// }

// 	// test("Multiple objects should persist correctly") {
// 	// 	val vec = List.fill(50)(Z(java.util.UUID.randomUUID.toString, "Should also be persisted"))
// 	// 	val retObj = for {
// 	// 		vs 		<- bucket <<* vec
// 	// 		retObj 	<- bucket ?* vec.map(_.id)
// 	// 	} yield retObj

// 	// 	val res = retObj.unsafeFulFill
// 	// 	assert(res.isSuccess && res.toOption.get.length == vec.length)
// 	// }

// 	// test("Objects should be deleted correctly") {
// 	// 	val newId = java.util.UUID.randomUUID.toString
// 	// 	val obj = Z(newId, "Should also be stored")

// 	// 	val retObj = for {
// 	// 		v 			<- bucket << obj
// 	// 		firstRet	<- bucket ? obj.id
// 	// 		_			<- bucket - obj
// 	// 		secRet 		<- bucket ? obj.id
// 	// 	} yield {
// 	// 		firstRet.isDefined && !secRet.isDefined 
// 	// 	} 

// 	// 	val res = retObj.unsafeFulFill
// 	// 	assert(res.isSuccess && res.toOption.get == true)
// 	// }

// 	// test("Fetching multiple deleted objects should return no values") {
// 	// 	val vec = List.fill(50)(Z(java.util.UUID.randomUUID.toString, "Should also be persisted"))
// 	// 	val retObj = for {
// 	// 		vs 		<- bucket <<* vec
// 	// 		bef 	<- bucket ?* vec.map(_.id)
// 	// 		_		<- bucket -* vec
// 	// 		aft		<- bucket ?* vec.map(_.id)
// 	// 	} yield {
// 	// 		bef.length == 50 && aft.length == 0
// 	// 	}

// 	// 	val res = retObj.unsafeFulFill
// 	// 	assert(res.isSuccess && res.toOption.get == true)
// 	// }
// }
