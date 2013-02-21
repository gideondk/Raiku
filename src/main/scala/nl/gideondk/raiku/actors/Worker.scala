package nl.gideondk.raiku.actors

import scalaz._
import Scalaz._

import akka.actor.IO.IterateeRef
import akka.actor._
import akka.util.ByteString
import akka.event.Logging

import akka.io._

import scala.concurrent.Promise

import java.net.InetSocketAddress

import akka.actor.SupervisorStrategy._
import akka.routing.RandomRouter
import akka.actor.OneForOneStrategy

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import akka.io.Tcp._
import nl.gideondk.raiku.commands.{ RiakResponse, RiakOperation }

private class RaikuWorkerActor(addr: InetSocketAddress) extends RaikuPBActor {
  val workerDescription = "Raiku client worker"
  val address = addr
  def specificMessageHandler: Receive = {
    case req @ RiakOperation(promise, command) ⇒
      tcpWorker match {
        case None ⇒ stash()
        case Some(w) ⇒
          for {
            _ ← state
            result ← Iteratees.readRiakResponse
          } yield {
            promise.success(result)
            result
          }
          w ! Write(command)
      }
  }
}

object Iteratees {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def readRiakResponse = for {
    frameLenBytes ← akka.actor.IO.take(4) // First 32 bits
    frameLen = frameLenBytes.iterator.getInt - 1
    messageType ← akka.actor.IO.take(1) // Second 8 bits
    frame ← akka.actor.IO.take(frameLen)
  } yield {
    RiakResponse(frameLen + 1, messageType.head.toInt, frame)
  }
}