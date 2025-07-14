package org.ekstep.analytics.dashboard.weekly.claps

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import java.sql.{Connection, DriverManager, Statement}
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JobLogger


object WeeklyClapsModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.weekly.claps.WeeklyClapsModel"

  override def name() = "WeeklyClapsModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
  try{
    val today = getDate()
    // get weekStart, weekEnd and dataTillDate(previous day) from today's date
    val (weekStart, weekEnd, weekEndTime, dataTillDate) = getThisWeekDates()
//    val weekStart = ""     //for manual testing
//    val weekEndTime = ""
    val appPostgresUrl =  s"jdbc:postgresql://${conf.appPostgresHost}/${conf.appPostgresSchema}"
    //get existing weekly-claps data
    val existingWeeklyClapsDF = cache.load("weeklyClaps")

    // get platform engagement data from summary-events druid datasource
    val platformEngagementDF = usersPlatformEngagementDataframe(weekStart, weekEndTime)

    val joinedWithExistingDF = existingWeeklyClapsDF.join(platformEngagementDF, Seq("userid"), "full")
      .withColumn("w4", struct(when(col("platformEngagementTime").isNull, 0).otherwise(col("platformEngagementTime")).alias("timespent"), when(col("sessionCount").isNull, 0)
        .otherwise(col("sessionCount")).alias("numberOfSessions")
      ))

    var df = joinedWithExistingDF

    val condition = col("w4")("timespent") >= conf.cutoffTime && !col("claps_updated_this_week")

    if(dataTillDate.equals(weekEnd) && !dataTillDate.equals(df.select(col("last_updated_on")))) {
      JobLogger.log("Started weekend updates")
      df = df.select(
        col("w2").alias("w1"),
        col("w3").alias("w2"),
        col("w4").alias("w3"),
        col("w4"),
        col("total_claps"),
        col("userid"),
        col("platformEngagementTime"),
        col("sessionCount"),
        col("claps_updated_this_week"),
        col("last_claps_updated_on")
      )
        .withColumn("total_claps", when(col("w4")("timespent") < conf.cutoffTime, 0).otherwise(col("total_claps")))
        .withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps")))
        .withColumn("last_updated_on", lit(dataTillDate))
        .withColumn("claps_updated_this_week", lit(false))
        .withColumn("w4",struct(
          lit(0.0).alias("timespent"),
          lit(0).alias("numberOfSessions")
        ))


      JobLogger.log("Completed weekend updates")

    } else {
      df = df.withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps")))
        .withColumn("claps_updated_this_week", when(condition, lit(true)).otherwise(col("claps_updated_this_week")))
    }

    df = df.withColumn("total_claps", when(col("total_claps").isNull, 0).otherwise(col("total_claps")))
      .withColumn("claps_updated_this_week", when(col("claps_updated_this_week").isNull, false).otherwise(col("claps_updated_this_week")))
      .withColumn("last_claps_updated_on", when(condition, currentDateTime).otherwise(col("last_claps_updated_on")))

    df = df.drop("platformEngagementTime","sessionCount")

    val finalDF =  df.withColumn("w1", safeToJson(df, "w1"))
      .withColumn("w2", safeToJson(df, "w2"))
      .withColumn("w3", safeToJson(df, "w3"))
      .withColumn("w4", safeToJson(df, "w4"))


    finalDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"/tmp/weeklyClaps${today}")
    truncateWarehouseTable(conf.dwLearnerStatsTable, appPostgresUrl)
    saveDataframeToPostgresTable_With_Append(finalDF, appPostgresUrl, conf.dwLearnerStatsTable, conf.appPostgresUsername, conf.appPostgresCredential)

  }catch {
    case e: Exception =>
      println(s"Error occurred during WeeklyClapsModel processing: ${e.getMessage}", e)
      System.exit(1)
  }
  }
  def safeToJson(df: DataFrame, colName: String): Column = {
    if (df.schema(colName).dataType.isInstanceOf[StructType]) {
      to_json(col(colName))
    } else {
      col(colName)
    }
  }
}