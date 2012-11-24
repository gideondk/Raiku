package nl.gideondk.raiku

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter
import akka.actor._
import scala.concurrent._
import scala.concurrent.duration._

import scalaz._
import Scalaz._

import spray.json._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

import scala.concurrent.duration._
import scala.concurrent._

class PerformanceSpec extends Specification with DefaultJsonProtocol {
	sequential 

	implicit val system = ActorSystem("perf-bucket-system")
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

  	val nrOfItems = 2000
  	
  	val randomObjects = List.fill(nrOfItems)(Y(java.util.UUID.randomUUID.toString, "Test Name", 25, "A")).toList
	val ids = randomObjects.map(_.id)

	val bucket = RaikuBucket[Y]("raiku_test_y_bucket", client)
	bucket.setBucketProperties(RaikuBucketProperties(None, Some(true))).unsafeFulFill

	"A bucket" should {
		"be able to store objects in timely fashion" in {
			val futs = bucket <<* (randomObjects, w = 1)
	      	val t1 = System.currentTimeMillis
	      	val status = futs.unsafeFulFill
	      	val t2 = System.currentTimeMillis
	     
	   	  	val diff = t2 - t1
	   	  	val nrOfOps = nrOfItems / (diff / 1000.0)
	   	  	
	   	  	println("Storing ops: " + nrOfOps)
	   	  	nrOfOps > 200
		}
		"be able to fetch objects in timely fashion" in {
	   	  val futs = bucket ?* (ids, r = 1)
	      val t1 = System.currentTimeMillis
	      val status = futs.unsafeFulFill.toOption.get
	      val t2 = System.currentTimeMillis

	   	  val diff = t2 - t1
	   	  val nrOfOps = nrOfItems / (diff / 1000.0)
	   	  
	   	  println("Fetching ops: " + nrOfOps)
	   	  nrOfOps > 500
		}
	}

	step { 
		client.disconnect
		system.shutdown() 
	}
}
