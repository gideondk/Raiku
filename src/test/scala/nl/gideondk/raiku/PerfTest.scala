// package nl.gideondk.raiku

// import org.scalatest.FunSuite
// import org.scalatest.BeforeAndAfter
// import akka.actor._
// import scala.concurrent._
// import scala.concurrent.duration._

// import scalaz._
// import Scalaz._

// class PerfTest extends FunSuite with BeforeAndAfter {
// 	implicit def intToIntOpt(a: Int): Option[Int] = a.some
	
// 	implicit val system = ActorSystem("system")
// 	var client:RaikuClient = _

// 	before {
// 		client = RaikuClient("localhost", 8087, 4)
// 	}

// 	after {
// 		client disconnect 
// 	}

// 	 val randomObjects = Seq.fill(4000)(RaikuRWObject("test_bucket", java.util.UUID.randomUUID.toString, "this should be stored".getBytes)).toList
// 	 val ids = randomObjects.map(_.key)

//  	test("Storing should be done in timely fashion") {
//    	  val futs = ValidatedFutureIO.sequence(randomObjects.map(client.storeRWObject(_, w = 1)))
//       val t1 = System.currentTimeMillis
//       val status = futs.unsafeFulFill(20 seconds).toOption.get
//       val t2 = System.currentTimeMillis
     
//    	  val diff = t2 - t1
//    	  val nrOfOps = status.length.toFloat / (diff / 1000.0)
   	  
//    	  println("Storing ops: " + nrOfOps)
//  	}

//  	test("Fetching should be done in timely fashion") {
//    	  val futs = ValidatedFutureIO.sequence(ids.map(id => client.fetchRWObject("test_bucket", id, r = 1)))
//       val t1 = System.currentTimeMillis
//       val status = futs.unsafeFulFill(20 seconds).toOption.get
//       val t2 = System.currentTimeMillis

//    	  val diff = t2 - t1
//    	  val nrOfOps = status.length.toFloat / (diff / 1000.0)
   	  
//    	  println("Fetching ops: " + nrOfOps)
//  	}

//  	test("Deleting should be done in timely fashion") {
//    	  val futs = ValidatedFutureIO.sequence(ids.map(id => client.deleteByKey("test_bucket", id, r = 1, w = 1)))
//       val t1 = System.currentTimeMillis
//       val status = futs.unsafeFulFill(20 seconds).toOption.get
//       val t2 = System.currentTimeMillis

//    	  val diff = t2 - t1
//    	  val nrOfOps = status.length.toFloat / (diff / 1000.0)
   	  
//    	  println("Fetching ops: " + nrOfOps)
//  	}
// }
