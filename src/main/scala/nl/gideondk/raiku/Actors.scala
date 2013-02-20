package nl.gideondk.raiku

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
import akka.routing.{ RandomRouter, RoundRobinRouter }
import akka.io.Tcp._
import akka.io.Tcp.Connected
import akka.io.Tcp.Register
import akka.io.Tcp.Connect
import akka.actor.OneForOneStrategy
import akka.io.Tcp.ErrorClosed
import akka.io.Tcp.Received
import collection.mutable

private[raiku] case class RiakResponse(length: Int, messageType: Int, message: ByteString)

private[raiku] case class RiakOperation(promise: Promise[RiakResponse], bytes: ByteString)

case class RaikuHost(host: String, port: Int)

case class RaikuConfig(host: RaikuHost, connections: Int)

object RaikuActor {
  val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2) {
      case _: RaikuWorkerDisconnectedUnexpectedlyException ⇒ Restart
      case _: RaikuWorkerNoTCPException ⇒ Restart
      case _: NullPointerException ⇒ Restart
      case _ ⇒ Escalate
    }
}

private class RaikuActor(config: RaikuConfig) extends Actor {
  val log = Logging(context.system, this)
  val address = new InetSocketAddress(config.host.host, config.host.port)

  val tcp = akka.io.IO(Tcp)(context.system)
  val router = context.system.actorOf(Props(new RaikuWorkerActor(tcp, address)).withRouter(RandomRouter(nrOfInstances = config.connections, supervisorStrategy = RaikuActor.supervisorStrategy)))

  def receive = {
    case req @ RiakOperation(promise, command) ⇒
      router forward req
  }
}

private class RaikuWorkerActor(tcp: ActorRef, address: InetSocketAddress) extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global

  val state = IterateeRef.async
  var tcpWorker: ActorRef = _
  val log = Logging(context.system, this)

  val queue: mutable.Queue[RiakOperation] = mutable.Queue[RiakOperation]()

  override def preStart = {
    tcp ! Connect(address)
  }

  override def postStop = {
    tcp ! Close
    state(akka.actor.IO.EOF)
  }

  def receive = {
    case Connected(remoteAddr, localAddr) ⇒
      sender ! Register(self)
      tcpWorker = sender
      log.debug("Raiku client worker connected to "+remoteAddr)
      if (queue.length > 0)
        queue.dequeueAll { req ⇒
          self ! req
          true
        }

    case ErrorClosed(cause) ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+") with cause: "+cause)
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case PeerClosed ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+")")
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case ConfirmedClosed ⇒
      log.debug("Raiku client worker disconnected from Riak ("+address+")")

    case m: ConnectionClosed ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+")") // TODO: handle the specific cases
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case Received(bytes: ByteString) ⇒
      state(akka.actor.IO.Chunk(bytes))

    case req @ RiakOperation(promise, command) ⇒
      if (tcpWorker != null) {
        for {
          _ ← state
          result ← Iteratees.readRiakResponse
        } yield {
          promise.success(result)
          result
        }

        tcpWorker ! Write(command)
      }
      else {
        queue += req
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

trait RaikuWorkerDisconnectedException extends Exception

case class RaikuWorkerDisconnectedUnexpectedlyException extends RaikuWorkerDisconnectedException

case class RaikuWorkerDisconnectedExpectedly extends RaikuWorkerDisconnectedException

case class RaikuWorkerNoTCPException extends Exception
