package bike.rapido.dq.notify

object JsonUtil {
  def esc(s: String): String = s.flatMap {
    case '"' => "\\\""
    case '\\' => "\\\\"
    case '\n' => "\\n"
    case '\r' => "\\r"
    case '\t' => "\\t"
    case c    => c.toString
  }
  def str(s: String): String = "\"" + esc(s) + "\""
  def obj(fields: (String, Any)*): String = {
    val body = fields.collect { case (k, v) => str(k) + ":" + value(v) }.mkString(",")
    "{" + body + "}"
  }
  def arr(items: Seq[Any]): String = items.map(value).mkString("[", ",", "]")
  def value(v: Any): String = v match {
    case s: String => str(s)
    case d: Double => if (d.isNaN) "null" else d.toString
    case i: Int => i.toString
    case b: Boolean => b.toString
    case m: Map[_, _] => obj(m.toSeq.asInstanceOf[Seq[(String, Any)]]: _*)
    case seq: Seq[_] => arr(seq.asInstanceOf[Seq[Any]])
    case None => "null"
    case Some(x) => value(x)
    case other => str(other.toString)
  }
}
