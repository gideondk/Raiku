package nl.gideondk.raiku

import scalaz._
import Scalaz._

import akka.actor.IO.{ IterateeRef, SocketHandle }
import akka.actor.{ IOManager, IO, Actor, ActorRef, ActorSystem }
import akka.routing._
import akka.util.ByteString
import akka.event.Logging
import collection.mutable

import scala.concurrent.Promise

case class RaikuHost(host: String, port: Int)
case class RaikuConfig(hosts: List[RaikuHost], connections: Int)

private[raiku] class RaikuActor(config: RaikuConfig) extends Actor {
  val log = Logging(context.system, this)
  val state = IterateeRef.Map.async[IO.Handle]()(context.dispatcher)

  var sockets: Vector[IO.SocketHandle] = _
  var nextSocketIndex: Int = 0
  var nextHostIndex: Int = 0

  var requests = 0L

  def connections = config.connections

  def nextHost = {
    val host = config.hosts(nextHostIndex)
    nextHostIndex = (nextHostIndex + 1) % config.hosts.length
    host
  }

  override def preStart = {
    log info ("Connecting")
    sockets = Vector.fill(connections) {
      val host = nextHost
      IOManager(context.system).connect(host.host, host.port)
    }
  }

  override def postStop() {
    log info ("Shutting down")
    sockets.foreach(_.close)
  }

  def removeSocket(socket: IO.SocketHandle) = {
    sockets = sockets diff Vector(socket)
  }

  def nextSocket = {
    if (nextSocketIndex > sockets.length) {
      nextSocketIndex = 0
    }
    val socket = sockets(nextSocketIndex)
    nextSocketIndex = (nextSocketIndex + 1) % sockets.length
    socket
  }

  def checkAndReinitSockets = if (sockets.length < math.ceil(config.connections / 2)) {
    val nrDiff = config.connections - sockets.length
    sockets = sockets ++ Vector.fill(nrDiff) {
      val host = nextHost
      IOManager(context.system).connect(host.host, host.port)
    }
  }

  def receive = {
    case IO.Connected(socket, address) ⇒
      log.info("Successfully connected to "+address)

    case IO.Read(socket, bytes) ⇒
      state(socket)(IO Chunk bytes)

    case IO.Closed(socket: IO.SocketHandle, cause) ⇒ {
      removeSocket(socket)
      log.info("Socket has closed, cause: "+cause)
    }

    case req @ RiakOperation(promise, bytes) ⇒
      val socket = nextSocket
      socket write bytes
      onRequest(req)
      for {
        _ ← state(socket)
        result ← Iteratees.readRiakResponse
      } yield {
        promise success result
      }

  }

  def onRequest(req: RiakOperation): Unit = {
    requests += 1L
  }
}

object Iteratees {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  final val readRiakResponse = for {
    frameLenBytes ← IO.take(4) // First 32 bits
    frameLen = frameLenBytes.iterator.getInt - 1
    messageType ← IO.take(1) // Second 8 bits
    frame ← IO.take(frameLen)
  } yield {
    RiakResponse(frameLen + 1, messageType.head.toInt, frame)
  }
}

private[raiku] case class RiakResponse(length: Int, messageType: Int, message: ByteString)
private[raiku] case class RiakOperation(promise: Promise[RiakResponse], bytes: ByteString)
