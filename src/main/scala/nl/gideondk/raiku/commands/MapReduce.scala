package nl.gideondk.raiku.commands

import com.basho.riak.protobuf._
import spray.json._

import nl.gideondk.raiku.mapreduce._
import nl.gideondk.raiku.mapreduce.MapReduceJsonProtocol._

trait MapReduce extends MRRequest {
  def mapReduce(job: MapReduceJob) = {
    buildMRRequest(request(RiakMessageType.RpbMapRedReq, RpbMapRedReq(job.toJson.compactPrint, "application/json")))
  }
}