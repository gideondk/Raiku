package nl.gideondk.raiku.mapreduce

import spray.json.{ DefaultJsonProtocol, JsArray, JsBoolean, JsNumber, JsObject, JsString, JsValue, RootJsonWriter }

object MapReduceJsonProtocol extends DefaultJsonProtocol {

  implicit object MapReducePhaseJsonFormat extends RootJsonWriter[MapReducePhase] {
    def write(p: MapReducePhase) = {
      val jsMap = Map(
        "language" -> JsString("javascript"),
        p.fn match {
          case x: BuildInMapReduceFunction ⇒
            "name" -> JsString(x.value)
          case x: MapReduceFunction ⇒
            "source" -> JsString(x.value)
        }) +
        (if (p.isInstanceOf[NonKeepedMapReducePhase]) ("keep" -> JsBoolean(false)) else ("keep" -> JsBoolean(true))) ++
        p.args.map(x ⇒ Map("args" -> x)).getOrElse(Map.empty[String, JsValue])
      JsObject(jsMap)
    }
  }

  implicit def phasesListFormat[T <: MapReducePhase] = new RootJsonWriter[List[T]] {
    def write(phases: List[T]) = JsArray(
      phases.map {
        _ match {
          case x: MPhase ⇒ JsObject("map" -> MapReducePhaseJsonFormat.write(x))
          case x: RPhase ⇒ JsObject("reduce" -> MapReducePhaseJsonFormat.write(x))
        }
      })
  }

  implicit object BucketMapReduceInputJsonFormat extends RootJsonWriter[BucketMapReduceInput] {
    def write(p: BucketMapReduceInput) = {
      p.keyFilters match {
        case None ⇒ JsString(p.bucket)
        case Some(filters) ⇒
          JsObject(
            "bucket" -> JsString(p.bucket),
            "key_filters" -> JsArray(filters.map(x ⇒ JsArray(x.map(JsString(_)).toList)).toList))
      }
    }
  }

  implicit object ItemMapReduceInputJsonFormat extends RootJsonWriter[ItemMapReduceInput] {
    def write(p: ItemMapReduceInput) = {
      JsArray(p.objs.map(tpl ⇒ JsArray(JsString(tpl._1), JsString(tpl._2))).toList)
    }
  }

  implicit object AnnotatedMapReduceInputJsonFormat extends RootJsonWriter[AnnotatedMapReduceInput] {
    def write(p: AnnotatedMapReduceInput) = {
      JsArray(p.objs.map(tpl ⇒ JsArray(JsString(tpl._1), JsString(tpl._2), JsString(tpl._3))).toList)
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
  //  implicit def mapReduceJobJsonFormat[T] = new RootJsonWriter[MapReduceJob[T]] {
  //    def write(p: MapReduceJob[T])(implicit tl: shapeless.ToList[T, MapReducePhase]) = {
  //      JsObject(
  //        p.input match {
  //          case x: BucketMapReduceInput    ⇒ "inputs" -> BucketMapReduceInputJsonFormat.write(x)
  //          case x: ItemMapReduceInput      ⇒ "inputs" -> ItemMapReduceInputJsonFormat.write(x)
  //          case x: AnnotatedMapReduceInput ⇒ "inputs" -> AnnotatedMapReduceInputJsonFormat.write(x)
  //          case x: BinIdxMapReduceInput    ⇒ "inputs" -> BinIdxMapReduceInputJsonFormat.write(x)
  //          case x: IntIdxMapReduceInput    ⇒ "inputs" -> IntIdxMapReduceInputJsonFormat.write(x)
  //        },
  //        "query" -> phasesListFormat.write(p.phases.toList),
  //        "timeout" -> JsNumber(p.timeout.toMillis))
  //    }
  //  }
}
