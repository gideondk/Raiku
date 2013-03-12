package nl.gideondk.raiku.actors

import java.net.InetSocketAddress

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart
import akka.actor.Terminated
import akka.event.Logging
import akka.routing.RandomRouter
import nl.gideondk.raiku.commands.RiakMROperation
import nl.gideondk.raiku.commands.RiakOperation

case class RaikuHost(host: String, port: Int)

case class RaikuConfig(host: RaikuHost, connections: Int, mrConnections: Int, reconnectDelay: FiniteDuration = 2 seconds)

object RaikuActor {
  val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2) {
      case _ ⇒ Restart
    }
}

private[raiku] class RaikuActor(config: RaikuConfig) extends Actor {
  import context.dispatcher
  val log = Logging(context.system, this)
  val address = new InetSocketAddress(config.host.host, config.host.port)
  var router: Option[ActorRef] = None
  var mrRouter: Option[ActorRef] = None

  def initialize {
    router = Some(context.system.actorOf(Props(new RaikuWorkerActor(address))
      .withRouter(RandomRouter(nrOfInstances = config.connections, supervisorStrategy = RaikuActor.supervisorStrategy))
      .withDispatcher("nl.gideondk.raiku.raiku-dispatcher")))
    context.watch(router.get)
  }

  def initializeMR {
    mrRouter = Some(context.system.actorOf(Props(new RaikuMRWorkerActor(address))
      .withRouter(RandomRouter(nrOfInstances = config.connections, supervisorStrategy = RaikuActor.supervisorStrategy))
      .withDispatcher("nl.gideondk.raiku.raiku-dispatcher")))
    context.watch(mrRouter.get)
  }

  def receive = {
    case InitializeRouters ⇒
      log.debug("Raiku routers initializing")
      initialize
      initializeMR

    case ReconnectRouter ⇒
      if (router.isEmpty) initialize

    case ReconnectMRRouter ⇒
      if (mrRouter.isEmpty) initializeMR

    case Terminated(actor) ⇒
      if (Some(actor) == router) {
        router = None
        log.debug("Raiku router died, restarting in: "+config.reconnectDelay.toString())
        context.system.scheduler.scheduleOnce(config.reconnectDelay, self, ReconnectRouter)
      }
      else if (Some(actor) == mrRouter) {
        mrRouter = None
        log.debug("Raiku MR router died, restarting in: "+config.reconnectDelay.toString())
        context.system.scheduler.scheduleOnce(config.reconnectDelay, self, ReconnectMRRouter)
      }

    case req @ RiakOperation(promise, command) ⇒
      router match {
        case Some(r) ⇒ r forward req
        case None    ⇒ promise.failure(NoConnectionException())
      }

    case req: RiakMROperation ⇒
      mrRouter match {
        case Some(r) ⇒ r forward req
        case None    ⇒ req.promise.failure(NoConnectionException())
      }
  }
}

trait WorkerDisconnectedException extends Exception

case class WorkerDisconnectedUnexpectedlyException extends WorkerDisconnectedException

case class WorkerDisconnectedExpectedly extends WorkerDisconnectedException

case class NoConnectionException extends Exception

case object InitializeRouters

case object ReconnectRouter

case object ReconnectMRRouter

