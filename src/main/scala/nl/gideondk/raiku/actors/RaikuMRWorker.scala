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

  var currentMRPhase: Option[Int] = None

  var phaseChannels = List.fill(phaseCount)(Channel())
  var currentPhaseChannelIndex = 0

  def newPromiseForHeadChannel = newPromiseForChannel(currentPhaseChannelIndex)

  def newPromiseForChannel(idx: Int) = {
    val c = phaseChannels(idx)
    if (c.hook.isDefined) {
      phaseChannels = phaseChannels.updated(currentPhaseChannelIndex, c.copy(hook = None))
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

  def enumeratorsForPhases = phaseChannels.zipWithIndex.map {
    case (e, i) ⇒
      implicit val timeout = Timeout(maxJobDuration)
      Enumerator.generateM { (self ? NextMRChunk(i)).mapTo[Promise[Option[JsValue]]].flatMap(_ future) }
  }

  def receive = {
    case StartMRProcessing ⇒
      e |>>> Iteratee.foreach { x ⇒
        self ! HandleMRChunk(x)
      }
      sender ! enumeratorsForPhases

    case NextMRChunk(idx: Int) ⇒
      val c = phaseChannels(idx)

      sender ! {
        if (c.queue.length == 0) {
          val p = Promise[Option[JsValue]]()
          phaseChannels = phaseChannels.updated(idx, c.copy(hook = Some(p)))
          p
        }
        else c.queue.dequeue()
      }

    case HandleMRChunk(chunk) ⇒
      chunk.messageType match {
        case RiakMessageType.RpbErrorResp ⇒
          val exception = new Exception(RpbErrorResp().mergeFrom(chunk.message.toArray).errmsg.toStringUtf8)
          phaseChannels.foreach { x ⇒
            x.hook.foreach(x ⇒ if (!x.isCompleted) x.failure(exception))
            x.queue.headOption.foreach(x ⇒ if (!x.isCompleted) x.failure(exception))
          }
          throw exception

        case RiakMessageType.RpbMapRedResp ⇒
          val mrResp = RpbMapRedResp().mergeFrom(chunk.message.toArray)
          if (mrResp.done.isDefined) {
            newPromiseForHeadChannel success None
          }
          else if (currentMRPhase.isDefined && currentMRPhase.get == mrResp.phase.get) {
            mrRespToJsValue(mrResp).foreach(x ⇒ newPromiseForHeadChannel success Some(x))
          }
          else if (currentMRPhase.isDefined && currentMRPhase.get != mrResp.phase.get) {
            newPromiseForHeadChannel success None
            currentPhaseChannelIndex = currentPhaseChannelIndex + 1
            mrRespToJsValue(mrResp).foreach(x ⇒ newPromiseForHeadChannel success Some(x))
            currentMRPhase = mrResp.phase
          }
          else {
            mrRespToJsValue(mrResp).foreach(x ⇒ newPromiseForHeadChannel success Some(x))
            currentMRPhase = mrResp.phase
          }

        case _ ⇒
          val exception = new Exception("Unexpect response returned in MR stream.")
          phaseChannels.foreach { x ⇒
            x.hook.foreach(x ⇒ if (!x.isCompleted) x.failure(exception))
            x.queue.headOption.foreach(x ⇒ if (!x.isCompleted) x.failure(exception))
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