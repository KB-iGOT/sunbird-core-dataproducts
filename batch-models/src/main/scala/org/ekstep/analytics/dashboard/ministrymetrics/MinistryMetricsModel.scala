package org.ekstep.analytics.dashboard.ministrymetrics

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object MinistryMetricsModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.MinistryMetricsModel"

  override def name() = "MinistryMetricsModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
try{
    val org_hierarchyDF = cache.load("orgHierarchy")
    val ministryNamesDF = org_hierarchyDF.select(col("mdo_name").alias("ministry"), col("mdo_id").alias("ministryID"))
    val enrolmentDF = warehouseCache.load("user_enrolments")
    val userDF = warehouseCache.load("user_detail").withColumnRenamed("mdo_id", "user_org_id").withColumnRenamed("user_id", "user_ID").filter(col("status") === 1)
    val query = """SELECT DISTINCT(uid) as user_ID FROM \"summary-events\" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR"""
    val usersLoggedInLast24HrsDF = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    val twentyFoutHrActiveUserDF = userDF.join(usersLoggedInLast24HrsDF, Seq("user_ID"), "inner")
    val joined24HrActiveUserDF = twentyFoutHrActiveUserDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
    val twentyFourHrActiveUserCountMinistryDF = joined24HrActiveUserDF
      .groupBy("ministry")
      .agg(count("user_ID").alias("activeUserCount"))
    val twentyFourHrActiveUserCountDeptDF = joined24HrActiveUserDF
      .groupBy("department")
      .agg(count("user_ID").alias("activeUserCount"))
      .select(col("department").alias("ministry"), col("activeUserCount"))
    val twentyFourHrActiveUserCountOrgDF = joined24HrActiveUserDF
      .groupBy("mdo_id")
      .agg(count("user_ID").alias("activeUserCount"))
      .select(col("mdo_id").alias("ministry"), col("activeUserCount"))
    val twentyFourHrActiveUserCountDF = twentyFourHrActiveUserCountMinistryDF.union(twentyFourHrActiveUserCountDeptDF).union(twentyFourHrActiveUserCountOrgDF)
    // Join the user and enrolment data
    val joinUserDF = enrolmentDF.join(userDF, enrolmentDF("user_id") === userDF("user_ID"), "inner") // Inner join on user_id

    // Join with the org_hierarchy data to get ministryID for all DF operations

    val joinedWithMinistryIDDF = joinUserDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
    val certificateMinistryDF = joinedWithMinistryIDDF
      .groupBy("ministry")
      .agg(countDistinct("certificate_id").alias("certificateCount"))

    val certificateDeptDF = joinedWithMinistryIDDF
      .groupBy("department")
      .agg(countDistinct("certificate_id").alias("certificateCount"))
      .select(col("department").alias("ministry"), col("certificateCount"))

    val certificateOrgDF = joinedWithMinistryIDDF
      .groupBy("mdo_id")
      .agg(countDistinct("certificate_id").alias("certificateCount"))
      .select(col("mdo_id").alias("ministry"), col("certificateCount"))


    val certificateResultDF = certificateMinistryDF.union(certificateDeptDF).union(certificateOrgDF)
    // Aggregate and create enrolmentResultDF

    val enrolmentMinistrytDF = joinedWithMinistryIDDF
      .groupBy("ministry")
      .agg(count("user_ID").alias("enrolmentCount"))

    val enrolmentDeptDF = joinedWithMinistryIDDF
      .groupBy("department")
      .agg(count("user_ID").alias("enrolmentCount"))
      .select(col("department").alias("ministry"), col("enrolmentCount"))

    val enrolmentOrgDF = joinedWithMinistryIDDF
      .groupBy("mdo_id")
      .agg(count("user_ID").alias("enrolmentCount"))
      .select(col("mdo_id").alias("ministry"), col("enrolmentCount"))

    val enrolmentResultDF = enrolmentMinistrytDF.union(enrolmentDeptDF).union(enrolmentOrgDF)
    // Create userCountDF
    val userCountMinistryDF = userDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("ministry")
      .agg(count("user_ID").alias("userCount"))

    val userCountDeptDF = userDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("department")
      .agg(count("user_ID").alias("userCount"))
      .select(col("department").alias("ministry"), col("userCount"))

    val userCountOrgDF = userDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("mdo_id")
      .agg(count("user_ID").alias("userCount"))
      .select(col("mdo_id").alias("ministry"), col("userCount"))

    val userCountDF = userCountMinistryDF.union(userCountDeptDF).union(userCountOrgDF)
    val finalActiveUserCountDF = twentyFourHrActiveUserCountDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("activeUserCount"), lit(0)).alias("activeUserCount"))
    val finalCertificateCountDF = certificateResultDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("certificateCount"), lit(0)).alias("certificateCount"))
    val finalUserCountDF = userCountDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("userCount"), lit(0)).alias("userCount"))
    val finalEnrolmentCountDF = enrolmentResultDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("enrolmentCount"), lit(0)).alias("enrolmentCount"))

    Redis.dispatchDataFrame[Long]("dashboard_rolled_up_login_percent_last_24_hrs", finalActiveUserCountDF, "ministryID", "activeUserCount")
    Redis.dispatchDataFrame[Int]("dashboard_rolled_up_user_count", finalUserCountDF, "ministryID", "userCount")
    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_certificates_generated_count", finalCertificateCountDF, "ministryID", "certificateCount")
    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_enrolment_content_count",finalEnrolmentCountDF, "ministryID", "enrolmentCount")
}catch {
  case e: Exception =>
    // Log the error
    println(s"Error occurred during DataExhaustModel processing: ${e.getMessage}", e)

    // Exit with status 1
    System.exit(1)
}
  }
}