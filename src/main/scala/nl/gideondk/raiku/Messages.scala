package nl.gideondk.raiku

trait Enum[A] {
  trait Value { self: A ⇒ }
  val values: List[A]
}

sealed trait RiakMessageType extends RiakMessageType.Value

object RiakMessageType extends Enum[RiakMessageType] {

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

  val values = List(RpbErrorResp, RpbPingReq, RpbPingResp, RpbGetClientIdReq, RpbGetClientIdResp, RpbSetClientIdReq, RpbSetClientIdResp, RpbGetServerInfoReq, RpbGetServerInfoResp,
    RpbGetReq, RpbGetResp, RpbPutReq, RpbPutResp, RpbDelReq, RpbDelResp, RpbListBucketsReq, RpbListBucketsResp,
    RpbListKeysReq, RpbListKeysResp, RpbGetBucketReq, RpbGetBucketResp, RpbSetBucketReq, RpbSetBucketResp,
    RpbMapRedReq, RpbMapRedResp, RpbIndexReq, RpbIndexResp, RpbSearchQueryReq, RbpSearchQueryResp)

  def messageTypeToInt(mt: RiakMessageType) = values.indexOf(mt)
  def intToMessageType(i: Int) = values(i)
}