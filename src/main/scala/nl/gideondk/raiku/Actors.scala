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
import akka.routing.RandomRouter
import akka.actor.OneForOneStrategy

import akka.io.Tcp._

private[raiku] case class RiakResponse(length: Int, messageType: Int, message: ByteString)

private[raiku] case class RiakOperation(promise: Promise[RiakResponse], bytes: ByteString)

case class RaikuHost(host: String, port: Int)

case class RaikuConfig(host: RaikuHost, connections: Int)

object RaikuActor {
  val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 8) {
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
  val router = context.system.actorOf(Props(new RaikuWorkerActor(tcp, address))
    .withRouter(RandomRouter(nrOfInstances = config.connections, supervisorStrategy = RaikuActor.supervisorStrategy))
    .withDispatcher("akka.actor.raiku-dispatcher"))

  def receive = {
    case req @ RiakOperation(promise, command) ⇒
      router forward req
  }
}

private class RaikuWorkerActor(tcp: ActorRef, address: InetSocketAddress) extends Actor with Stash {
  import scala.concurrent.ExecutionContext.Implicits.global

  val state = IterateeRef.async
  var tcpWorker: ActorRef = _
  val log = Logging(context.system, this)

  var connected = false

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
      connected = true
      unstashAll()
      log.debug("Raiku client worker connected to "+remoteAddr)

    case ErrorClosed(cause) ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+") with cause: "+cause)
      connected = false
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case PeerClosed ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+")")
      connected = false
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case ConfirmedClosed ⇒
      log.debug("Raiku client worker disconnected from Riak ("+address+")")
      connected = false

    case m: ConnectionClosed ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+")") // TODO: handle the specific cases
      connected = false
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case Received(bytes: ByteString) ⇒
      state(akka.actor.IO.Chunk(bytes))

    case req @ RiakOperation(promise, command) ⇒
      connected match {
        case false ⇒
          stash()
        case true ⇒
          for {
            _ ← state
            result ← Iteratees.readRiakResponse
          } yield {
            promise.success(result)
            result
          }

          tcpWorker ! Write(command)
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
