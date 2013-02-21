package nl.gideondk.raiku.mapreduce

import spray.json._

object MapReduceJsonProtocol extends DefaultJsonProtocol {
  implicit object MapReducePhaseJsonFormat extends RootJsonWriter[MapReducePhase] {
    def write(p: MapReducePhase) = {
      val jsMap = Map(
        "language" -> JsString("javascript"),
        p.fn match {
          case x: MRBuiltinFunction ⇒
            "name" -> JsString(x.value)
          case x: MRFunction ⇒
            "source" -> JsString(x.value)
        },
        "keep" -> JsBoolean(p.keep))
      JsObject(if (p.arg.isDefined) jsMap ++ Map("args" -> JsString(p.arg.get)) else jsMap)
    }
  }

  implicit object MapReducePipeJsonFormat extends RootJsonWriter[MapReducePipe] {
    def write(p: MapReducePipe) = {
      JsArray(
        p.phases.map {
          _ match {
            case x: MapPhase    ⇒ JsObject("map" -> MapReducePhaseJsonFormat.write(x))
            case x: ReducePhase ⇒ JsObject("reduce" -> MapReducePhaseJsonFormat.write(x))
          }
        } list)
    }
  }

  implicit object BucketMapReduceInputJsonFormat extends RootJsonWriter[BucketMapReduceInput] {
    def write(p: BucketMapReduceInput) = {
      JsObject(
        "bucket" -> JsString(p.bucket),
        "key_filters" -> JsArray(p.keyFilters.map(x ⇒ JsArray(x.map(JsString(_)).toList)).toList))
    }
  }

  implicit object ItemMapReduceInputJsonFormat extends RootJsonWriter[ItemMapReduceInput] {
    def write(p: ItemMapReduceInput) = {
      JsArray(p.objs.map(tpl ⇒ JsArray(JsString(tpl._1), JsString(tpl._2))).toList)
    }
  }

  implicit object BinIdxMapReduceInputJsonFormat extends RootJsonWriter[BinIdxMapReduceInput] {
    def write(p: BinIdxMapReduceInput) = {
      JsObject(
        "bucket" -> JsString(p.bucket),
        "index" -> JsString(p.indexKey),
        "value" -> JsString(p.indexValue))
    }
  }

  implicit object IntIdxMapReduceInputJsonFormat extends RootJsonWriter[IntIdxMapReduceInput] {
    def write(p: IntIdxMapReduceInput) = {
      p.indexValue match {
        case Left(x) ⇒
          JsObject(
            "bucket" -> JsString(p.bucket),
            "index" -> JsString(p.indexKey),
            "value" -> JsNumber(x))
        case Right(x) ⇒
          JsObject(
            "bucket" -> JsString(p.bucket),
            "index" -> JsString(p.indexKey),
            "start" -> JsNumber(x.head),
            "end" -> JsNumber(x.last))
      }
    }
  }

  implicit object AnnotatedMapReduceInputJsonFormat extends RootJsonWriter[AnnotatedMapReduceInput] {
    def write(p: AnnotatedMapReduceInput) = {
      JsArray(p.objs.map(tpl ⇒ JsArray(JsString(tpl._1), JsString(tpl._2), JsString(tpl._3))).toList)
    }
  }

  implicit object MapReduceJobJsonFormat extends RootJsonWriter[MapReduceJob] {
    def write(p: MapReduceJob) = {
      JsObject(
        p.input match {
          case x: ItemMapReduceInput ⇒ "inputs" -> ItemMapReduceInputJsonFormat.write(x)
        },
        "query" -> MapReducePipeJsonFormat.write(p.mrPipe),
        "timeout" -> JsNumber(p.timeout.toMillis))
    }
  }
}
