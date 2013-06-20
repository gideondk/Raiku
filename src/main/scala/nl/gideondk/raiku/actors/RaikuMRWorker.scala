package nl.gideondk.raiku.actors

import nl.gideondk.raiku.commands._
import nl.gideondk.sentinel.client.SentinelClient
import com.basho.riak.protobuf._
import akka.io._
import akka.actor.ActorSystem
import akka.util.ByteString
import akka.util.ByteStringBuilder
import play.api.libs.iteratee._
import spray.json._
import nl.gideondk.sentinel.pipelines.EnumeratorStage
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Actor
import scala.concurrent.Promise
import akka.pattern._

import akka.util.Timeout
import scala.concurrent.duration._

case object StartMRProcessing

class MRResultHandler(e: Enumerator[RiakResponse], phaseCount: Int, maxJobDuration: FiniteDuration = 5 minutes) extends Actor {
  case class HandleMRChunk(chunk: RiakResponse)
  case class NextMRChunk(i: Int)

  case class Channel(hook: Option[Promise[Option[JsValue]]] = None,
                     queue: scala.collection.mutable.Queue[Promise[Option[JsValue]]] = scala.collection.mutable.Queue[Promise[Option[JsValue]]]())

  var phaseChannels = List.fill(phaseCount)(Channel())

  var currentPhaseIndex = 0
  var currentPhase: Option[Int] = None

  def promiseForHeadChannel = promiseForChannel(currentPhaseIndex)

  def promiseForChannel(idx: Int) = {
    val c = phaseChannels(idx)
    if (c.hook.isDefined) {
      phaseChannels = phaseChannels.updated(currentPhaseIndex, c.copy(hook = None))
      c.hook.get
    }
    else {
      val p = Promise[Option[JsValue]]()
      c.queue.enqueue(p)
      p
    }
  }

  def mrRespToJsValue(resp: RpbMapRedResp) =
    JsonParser(resp.response.get.toStringUtf8).asInstanceOf[JsArray].elements // TODO: Less assumptions!

  def receive = {
    case StartMRProcessing ⇒
      e |>>> Iteratee.foreach { x ⇒
        self ! HandleMRChunk(x)
      }

      val enums = phaseChannels.zipWithIndex.map {
        case (e, i) ⇒
          implicit val timeout = Timeout(maxJobDuration)
          Enumerator.generateM { (self ? NextMRChunk(i)).mapTo[Promise[Option[JsValue]]].flatMap(_ future) }
      }
      sender ! enums

    case NextMRChunk(idx: Int) ⇒
      val c = phaseChannels(idx)
        def addHookForIdentifier = {
          val p = Promise[Option[JsValue]]()
          phaseChannels = phaseChannels.updated(idx, c.copy(hook = Some(p)))
          p
        }

      sender ! {
        if (c.queue.length == 0) addHookForIdentifier
        else c.queue.dequeue()
      }

    case HandleMRChunk(chunk) ⇒
      chunk.messageType match {
        case RiakMessageType.RpbErrorResp ⇒
          val exception = new Exception(RpbErrorResp().mergeFrom(chunk.message.toArray).errmsg.toStringUtf8)
          phaseChannels.foreach { x ⇒
            x.hook.foreach(_.failure(exception))
            x.queue.headOption.foreach(_.failure(exception))
          }
          throw exception

        case RiakMessageType.RpbMapRedResp ⇒
          val mrResp = RpbMapRedResp().mergeFrom(chunk.message.toArray)
          if (mrResp.done.isDefined) {
            promiseForHeadChannel success None
          }
          else if (currentPhase.isDefined && currentPhase.get == mrResp.phase.get) {
            mrRespToJsValue(mrResp).foreach(x ⇒ promiseForHeadChannel success Some(x))
          }
          else if (currentPhase.isDefined && currentPhase.get != mrResp.phase.get) {
            promiseForHeadChannel success None
            currentPhaseIndex = currentPhaseIndex + 1
            mrRespToJsValue(mrResp).foreach(x ⇒ promiseForHeadChannel success Some(x))
            currentPhase = mrResp.phase
          }
          else {
            mrRespToJsValue(mrResp).foreach(x ⇒ promiseForHeadChannel success Some(x))
            currentPhase = mrResp.phase
          }

        case _ ⇒
          val exception = new Exception("Unexpect response returned in MR stream.")
          phaseChannels.foreach { x ⇒
            x.hook.foreach(_.failure(exception))
            x.queue.headOption.foreach(_.failure(exception))
          }
          throw exception
      }
  }
}

object RaikuMRWorker {
  def isMREnd(rs: RiakResponse) = {
    RpbMapRedResp().mergeFrom(rs.message.toArray).done.isDefined
  }

  def apply(host: String, port: Int, numberOfWorkers: Int)(implicit system: ActorSystem) = {
      def stages = new EnumeratorStage(isMREnd, true) >> new RiakMessageStage >> new LengthFieldFrame(1024 * 1024 * 200, lengthIncludesHeader = false) // 200mb max
    SentinelClient.randomRouting(host, port, numberOfWorkers, "Raiku-MR")(stages)(system)
  }
}