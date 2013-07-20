package nl.gideondk.raiku.commands

sealed trait RiakMessageType

object RiakMessageType {

  case object RpbErrorResp extends RiakMessageType // 0

  case object RpbPingReq extends RiakMessageType // 1
  case object RpbPingResp extends RiakMessageType // 2

  case object RpbGetClientIdReq extends RiakMessageType // 3
  case object RpbGetClientIdResp extends RiakMessageType // 4

  case object RpbSetClientIdReq extends RiakMessageType // 5
  case object RpbSetClientIdResp extends RiakMessageType // 6

  case object RpbGetServerInfoReq extends RiakMessageType // 7
  case object RpbGetServerInfoResp extends RiakMessageType // 8

  case object RpbGetReq extends RiakMessageType // 9
  case object RpbGetResp extends RiakMessageType // 10
  case object RpbPutReq extends RiakMessageType // 11
  case object RpbPutResp extends RiakMessageType // 12
  case object RpbDelReq extends RiakMessageType // 13
  case object RpbDelResp extends RiakMessageType // 14

  case object RpbListBucketsReq extends RiakMessageType // 15
  case object RpbListBucketsResp extends RiakMessageType // 16

  case object RpbListKeysReq extends RiakMessageType // 17
  case object RpbListKeysResp extends RiakMessageType // 18

  case object RpbGetBucketReq extends RiakMessageType // 19
  case object RpbGetBucketResp extends RiakMessageType // 20
  case object RpbSetBucketReq extends RiakMessageType // 21
  case object RpbSetBucketResp extends RiakMessageType // 22

  case object RpbMapRedReq extends RiakMessageType // 23
  case object RpbMapRedResp extends RiakMessageType // 24

  case object RpbIndexReq extends RiakMessageType // 25
  case object RpbIndexResp extends RiakMessageType // 26

  case object RpbSearchQueryReq extends RiakMessageType // 27
  case object RbpSearchQueryResp extends RiakMessageType // 28

  case object RbpCounterUpdateReq extends RiakMessageType // 50
  case object RbpCounterUpdateResp extends RiakMessageType // 51
  case object RbpCounterGetReq extends RiakMessageType // 52
  case object RbpCounterGetResp extends RiakMessageType // 53

  val values = Map(0 -> RpbErrorResp, 1 -> RpbPingReq, 2 -> RpbPingResp, 3 -> RpbGetClientIdReq, 4 -> RpbGetClientIdResp, 5 -> RpbSetClientIdReq, 6 -> RpbSetClientIdResp, 7 -> RpbGetServerInfoReq, 8 -> RpbGetServerInfoResp,
    9 -> RpbGetReq, 10 -> RpbGetResp, 11 -> RpbPutReq, 12 -> RpbPutResp, 13 -> RpbDelReq, 14 -> RpbDelResp, 15 -> RpbListBucketsReq, 16 -> RpbListBucketsResp,
    17 -> RpbListKeysReq, 18 -> RpbListKeysResp, 19 -> RpbGetBucketReq, 20 -> RpbGetBucketResp, 21 -> RpbSetBucketReq, 22 -> RpbSetBucketResp,
    23 -> RpbMapRedReq, 24 -> RpbMapRedResp, 25 -> RpbIndexReq, 26 -> RpbIndexResp, 27 -> RpbSearchQueryReq, 28 -> RbpSearchQueryResp,
    50 -> RbpCounterUpdateReq, 51 -> RbpCounterUpdateResp, 52 -> RbpCounterGetReq, 53 -> RbpCounterGetResp)

  def messageTypeToInt(mt: RiakMessageType) = values.keys.toList(values.values.toList.indexOf(mt))
  def intToMessageType(i: Int) = values(i)
}