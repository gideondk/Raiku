package nl.gideondk.raiku.actors

import akka.io._

import akka.util.ByteString
import akka.util.ByteStringBuilder

import nl.gideondk.raiku.commands._

import nl.gideondk.sentinel.client.SentinelClient
import akka.actor.ActorSystem
import play.api.libs.iteratee._
import spray.json._
import com.basho.riak.protobuf._

class RiakMRMessageStage extends PipelineStage[HasByteOrder, (RiakCommand, List[Concurrent.Channel[JsValue]]), ByteString, Unit, ByteString] {
  var queue = scala.collection.mutable.Queue[List[Concurrent.Channel[JsValue]]]()
  var commandQueue = scala.collection.mutable.Queue[(RiakCommand, List[Concurrent.Channel[JsValue]])]()

  var currentChannels: Option[List[Concurrent.Channel[JsValue]]] = None
  var currentMRPhase: Option[Int] = None
  var currentChannelIndex = 0

  var isCurrentlyRunningMRJob = false // Riak doesn't like multiple requests in a pipline when MR responses are still processing.

  def mrRespToJsValue(resp: RpbMapRedResp) = {
    JsonParser(resp.response.get.toStringUtf8).asInstanceOf[JsArray].elements // TODO: Less assumptions!
  }

  def apply(ctx: HasByteOrder) = new PipePair[(RiakCommand, List[Concurrent.Channel[JsValue]]), ByteString, Unit, ByteString] {
    implicit val byteOrder = ctx.byteOrder

    override val commandPipeline = {
      msg: (RiakCommand, List[Concurrent.Channel[JsValue]]) ⇒
        if (!isCurrentlyRunningMRJob) {
          queue.enqueue(msg._2)
          val command = msg._1
          val bsb = new ByteStringBuilder
          bsb.putByte(RiakMessageType.messageTypeToInt(command.messageType).toByte)
          bsb ++= command.message
          isCurrentlyRunningMRJob = true
          ctx.singleCommand(bsb.result)
        }
        else {
          commandQueue.enqueue(msg)
          ctx.nothing
        }
    }

    override val eventPipeline = {
      bs: ByteString ⇒
        val channel = currentChannels match {
          case None ⇒
            val channels = queue.dequeue()
            currentChannels = Some(channels)
            channels(0)
          case Some(x) ⇒ x(currentChannelIndex) // Should be safe, or else pipeline integrity is comprimised anyway!
        }

        val bi = bs.iterator
        val messageType = bi.getByte
        val message = bi.toByteString

        RiakMessageType.intToMessageType(messageType.toInt) match {
          case RiakMessageType.RpbErrorResp ⇒
            val exception = new Exception(RpbErrorResp().mergeFrom(message.toArray).errmsg.toStringUtf8)
            currentChannels.foreach(_.foreach(channel ⇒ channel.end(exception)))
            throw exception

          case RiakMessageType.RpbMapRedResp ⇒
            val mrResp = RpbMapRedResp().mergeFrom(message.toArray)
            if (mrResp.done.isDefined) {
              channel.eofAndEnd()
              currentChannelIndex = 0
              currentChannels = None
              currentMRPhase = None
              isCurrentlyRunningMRJob = false
              if (commandQueue.length > 0) commandPipeline(commandQueue.dequeue)
              Nil
            }
            else if (currentMRPhase.isDefined && currentMRPhase.get == mrResp.phase.get) {
              mrRespToJsValue(mrResp).foreach(channel push)
              Nil
            }
            else if (currentMRPhase.isDefined && currentMRPhase.get != mrResp.phase.get) {
              channel.eofAndEnd()
              val newIndex = currentMRPhase.get + 1
              currentMRPhase = Some(newIndex)
              if (currentChannels.get.length > newIndex) mrRespToJsValue(mrResp).foreach(currentChannels.get(newIndex) push)
              currentChannelIndex += 1
              Nil
            }
            else {
              mrRespToJsValue(mrResp).foreach(channel push)
              currentMRPhase = mrResp.phase
              Seq(Left(())) // Return a value through the pipeline after initial contact is made (so the Task[Unit] resolves)
            }
        }
    }
  }
}

object RaikuMRWorker {
  def ctx = new HasByteOrder {
    def byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  }

  val stages = new RiakMRMessageStage >> new LengthFieldFrame(1024 * 1024 * 200, lengthIncludesHeader = false) // 200mb max

  def apply(host: String, port: Int, numberOfWorkers: Int)(implicit system: ActorSystem) = {
    SentinelClient.randomRouting(host, port, numberOfWorkers, "Raiku-MR")(ctx, stages)(system)
  }
}