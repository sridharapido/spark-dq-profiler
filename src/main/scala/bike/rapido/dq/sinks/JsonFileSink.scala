package bike.rapido.dq.sinks

import bike.rapido.dq.{MetricPoint, RuleResult}
import bike.rapido.dq.drift.DriftEvent
import org.apache.spark.sql.Dataset
import java.io.{File, FileWriter, PrintWriter}
import java.time.LocalDate

class JsonFileSink {
  private def outDir(): File = { val day = LocalDate.now().toString.replaceAll("-", ""); val dir = new File("out" + File.separator + day); if (!dir.exists()) dir.mkdirs(); dir }
  private def appendLines(path: File, lines: Iterator[String]): Unit = { val fw = new FileWriter(path, true); val pw = new PrintWriter(fw); try lines.foreach(pw.println) finally pw.close() }
  def writeMetrics(ds: Dataset[MetricPoint]): Unit = { val file = new File(outDir(), "metrics.json"); val it = ds.toLocalIterator(); val json = scala.collection.JavaConverters.asScalaIterator(it).map(JsonEncoding.toJson); appendLines(file, json) }
  def writeRules(ds: Dataset[RuleResult]): Unit = { val file = new File(outDir(), "rules.json"); val it = ds.toLocalIterator(); val json = scala.collection.JavaConverters.asScalaIterator(it).map(JsonEncoding.toJson); appendLines(file, json) }
  def writeDrift(ds: Dataset[DriftEvent]): Unit = { val file = new File(outDir(), "drift.json"); val it = ds.toLocalIterator(); val json = scala.collection.JavaConverters.asScalaIterator(it).map(JsonEncoding.toJson); appendLines(file, json) }
}

import scala.collection.JavaConverters._

