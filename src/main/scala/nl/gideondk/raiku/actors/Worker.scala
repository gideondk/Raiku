package nl.gideondk.raiku.actors

import java.net.InetSocketAddress

//import akka.actor.actorRef2Scala
//import akka.io.Tcp.Write
//import nl.gideondk.raiku.commands.RiakOperation
//import nl.gideondk.raiku.commands.RiakResponse
////
//private class RaikuWorkerActor(addr: InetSocketAddress) extends RaikuBaseActor {
//  val workerDescription = "Raiku client worker"
//  val address = addr
//  def specificMessageHandler: Receive = {
//    case req @ RiakOperation(promise, command) ⇒
//      tcpWorker match {
//        case None ⇒ stash()
//        case Some(w) ⇒
//          for {
//            _ ← state
//            result ← Iteratees.readRiakResponse
//          } yield {
//            promise.success(result)
//            result
//          }
//          write(command)
//      }
//  }
//}
//
//object Iteratees {
//  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
//
//  def readRiakResponse = for {
//    frameLenBytes ← akka.actor.IO.take(4) // First 32 bits
//    frameLen = frameLenBytes.iterator.getInt - 1
//    messageType ← akka.actor.IO.take(1) // Second 8 bits
//    frame ← akka.actor.IO.take(frameLen)
//  } yield {
//    RiakResponse(frameLen + 1, messageType.head.toInt, frame)
//  }
//}
