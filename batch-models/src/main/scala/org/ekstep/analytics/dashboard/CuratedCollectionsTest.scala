package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext

object CuratedCollectionsTest extends Serializable {
  def main(args: Array[String]): Unit = {
    val cassandraHost = testModelConfig().getOrElse("sparkCassandraConnectionHost", "localhost").asInstanceOf[String]
    val esHost = testModelConfig().getOrElse("sparkElasticsearchConnectionHost", "localhost").asInstanceOf[String]
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("CompetencyMetricsTest")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host", cassandraHost)
        .config("spark.cassandra.output.batch.size.rows", "10000")
        //.config("spark.cassandra.read.timeoutMS", "60000")
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true")
        .config("spark.sql.caseSensitive", "true")
        .config("es.read.field.as.array.include","author,client,project")
        .config("es.nodes", esHost)
        .config("es.port", "9200")
        .getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val fc: FrameworkContext = new FrameworkContext()
    sc.setLogLevel("WARN")
    val res = time(test());
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "10.0.0.5:9092",
      "compression" -> "snappy",
      "topics" -> Map(
        "allCourses" -> "dev.dashboards.course",
        "allResources" -> "dev.dashboards.resource",
        "courseDetails" -> "dev.dashboards.course.details",
        "userCourseProgress" -> "dev.dashboards.user.course.progress",
        "fracCompetency" -> "dev.dashboards.competency.frac",
        "courseCompetency" -> "dev.dashboards.competency.course",
        "expectedCompetency" -> "dev.dashboards.competency.expected",
        "declaredCompetency" -> "dev.dashboards.competency.declared",
        "competencyGap" -> "dev.dashboards.competency.gap"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "sparkCassandraConnectionHost" -> "10.0.0.7",
      "sparkDruidRouterHost" -> "10.0.0.13",
      "sparkElasticsearchConnectionHost" -> "10.0.0.7",
      "cassandraKeyspace" -> "dev_hierarchy_store",
      "cassandraTable" -> "content_hierarchy",
      "redisHost" -> "10.0.0.6",
      "redisPort" -> "6379",
      "redisDB" -> "12",
      "sideOutput" -> sideOutput
    )
    modelParams
  }

  def test()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    val timestamp = System.currentTimeMillis()
    val config = testModelConfig()
    CuratedCollectionsModel.processCuratedCollectionsData(timestamp, config)
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }
}
