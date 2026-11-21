// Spark SQL from Scala: the same queries, plus the tier UDF as a Scala function (no Python boundary).
// Run with: spark-shell --master local[6] --driver-memory 6g -i bench/RunSpark.scala  (env: DATA, SIZE, OUT)
import scala.io.Source
import java.nio.file.{Files, Paths}

val t0 = System.nanoTime
val data = sys.env("DATA"); val size = sys.env("SIZE"); val out = sys.env("OUT")
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "12")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.read.parquet(s"$data/calls_$size").createOrReplaceTempView("calls")
spark.read.parquet(s"$data/cells.parquet").createOrReplaceTempView("cells")
val startup = (System.nanoTime - t0) / 1e9  // after the shell is up; JVM start is measured by the wrapper

val text = Source.fromFile("bench/queries.sql").mkString
val qs = text.split("(?m)^-- name: ").drop(1).map { b => val i = b.indexOf('\n'); (b.take(i).trim, b.drop(i + 1).trim.stripSuffix(";")) }
spark.udf.register("tier_scala", (r: Double) => if (r >= 1.0) "high" else if (r >= 0.1) "mid" else "low")
val all = qs :+ ("udf_tier_scala", "SELECT tier, count(*) AS n FROM (SELECT tier_scala(revenue_try) AS tier FROM calls) GROUP BY tier ORDER BY tier")

def time(sql: String): (Double, Array[org.apache.spark.sql.Row]) = { val t = System.nanoTime; val r = spark.sql(sql).collect(); ((System.nanoTime - t) / 1e9, r) }
def q(s: String) = "\"" + s.replace("\"", "\\\"") + "\""
val parts = all.map { case (name, sql) =>
  val (first, _) = time(sql)
  val runs = (1 to 3).map(_ => time(sql))
  val med = runs.map(_._1).sorted.apply(1)
  val rows = runs.last._2.map(r => r.toSeq.map { case null => "null"; case s: String => q(s); case d: java.sql.Date => q(d.toString); case ld: java.time.LocalDate => q(ld.toString); case v => v.toString }.mkString("[", ",", "]")).mkString("[", ",", "]")
  println(f"scala $size $name $med%.4f")
  s"""${q(name)}: {"first_s": $first, "median_s": $med, "runs_s": ${runs.map(_._1).mkString("[", ",", "]")}, "rows": $rows}"""
}
val json = s"""{"engine": "spark-scala", "version": ${q(spark.version)}, "size": ${q(size)}, "cpus": 6, "startup_s": $startup, "queries": {${parts.mkString(",\n")}}}"""
Files.write(Paths.get(out), json.getBytes("UTF-8"))
System.exit(0)
