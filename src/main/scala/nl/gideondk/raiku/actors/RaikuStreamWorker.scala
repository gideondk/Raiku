package nl.gideondk.raiku.actors

import nl.gideondk.raiku.commands._
import nl.gideondk.sentinel.client.SentinelClient
import akka.io._
import akka.actor._
import akka.util._
import akka.routing._

import com.basho.riak.protobuf.RpbIndexResp
import nl.gideondk.sentinel.pipelines.EnumeratorStage

import akka.io.TcpPipelineHandler.{ Init, WithinActorContext }

import scala.concurrent.ExecutionContext.Implicits.global

object Raiku2iStreamWorker {
  def isSIEnd(rs: RiakResponse) =
    RpbIndexResp().mergeFrom(rs.message.toArray).done.isDefined

  def apply(host: String, port: Int, numberOfWorkers: Int)(implicit system: ActorSystem) = {
      def stages = new EnumeratorStage(isSIEnd, true) >> new RiakMessageStage >> new LengthFieldFrame(1024 * 1024 * 200, lengthIncludesHeader = false) // 200mb max
    SentinelClient.waiting(host, port, RandomRouter(numberOfWorkers), "Raiku-2i")(stages)(system)
  }
}
