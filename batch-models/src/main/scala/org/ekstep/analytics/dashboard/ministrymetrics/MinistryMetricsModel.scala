package org.ekstep.analytics.dashboard.ministrymetrics

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object MinistryMetricsModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.MinistryMetricsModel"

  override def name() = "MinistryMetricsModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    import spark.implicits._

    // Get user and org data
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyCompleteDF = orgCompleteHierarchyDataFrame()
    val distinctMdoIDsDF = broadcast(userOrgDF.select("userOrgID").distinct())
    println("Number of distinct MDO IDs: " + distinctMdoIDsDF.count())

    val joinedDF = orgHierarchyCompleteDF.join(distinctMdoIDsDF, orgHierarchyCompleteDF("sborgid") === distinctMdoIDsDF("userOrgID"), "inner").cache()
    println("Number of distinct orgs in orgHierarchy: " + joinedDF.count())
    show(joinedDF, "orgHierarchyCompleteDF")

    // Load Redis data as broadcasted DataFrames
    val userSumDF = broadcast(Redis.getMapAsDataFrame("dashboard_user_count_by_user_org", Schema.totalLearningHoursSchema))
    val userLoginPercentDF = broadcast(Redis.getMapAsDataFrame("dashboard_login_percent_last_24_hrs_by_user_org", Schema.totalLearningHoursSchema))
    val certSumDF = broadcast(Redis.getMapAsDataFrame("dashboard_certificates_generated_count_by_user_org", Schema.totalLearningHoursSchema))
    val enrolmentDF = broadcast(Redis.getMapAsDataFrame("dashboard_enrolment_content_by_user_org", Schema.totalLearningHoursSchema))


    def processOrgsL3(df: DataFrame): DataFrame = {

      val organisationDF = df.dropDuplicates()

      // Aggregate learning hours by organisation
      val userJoinedDF = organisationDF
        .join(userSumDF, col("userOrgID") === col("organisationID"), "left_outer")
        .groupBy("organisationID")
        .agg(sum(col("totalLearningHours")).alias("learningSumValue"))

      // Join with login data and aggregate login hours
      val loginJoinedDF = userJoinedDF
        .join(userLoginPercentDF, col("organisationID") === col("userOrgID"), "left_outer")
        .groupBy("organisationID", "learningSumValue")
        .agg(sum(col("totalLearningHours")).alias("loginSumValue"))

      // Join with certificates data and aggregate certificate hours
      val certJoinedDF = loginJoinedDF
        .join(certSumDF, col("organisationID") === col("userOrgID"), "left_outer")
        .groupBy("organisationID", "learningSumValue", "loginSumValue")
        .agg(sum(col("totalLearningHours")).alias("certSumValue"))

      // Join with enrolment data and aggregate enrolment hours
      val finalResultDF = certJoinedDF
        .join(enrolmentDF, col("organisationID") === col("userOrgID"), "left_outer")
        .groupBy("organisationID", "learningSumValue", "loginSumValue", "certSumValue")
        .agg(sum(col("totalLearningHours")).alias("enrolmentSumValue"))
        .withColumn("allIDs", lit(null).cast("string"))
        .select(
          col("organisationID").alias("ministryID"),
          col("allIDs"),
          col("learningSumValue"),
          col("loginSumValue"),
          col("certSumValue"),
          col("enrolmentSumValue")
        )

      show(finalResultDF, "finalresult")


      // Cast columns to integer and coalesce null values
      finalResultDF
        .withColumn("learningSumValue", coalesce(col("learningSumValue").cast("int"), lit(0)))
        .withColumn("loginSumValue", coalesce(col("loginSumValue").cast("int"), lit(0)))
        .withColumn("certSumValue", coalesce(col("certSumValue").cast("int"), lit(0)))
        .withColumn("enrolmentSumValue", coalesce(col("enrolmentSumValue").cast("int"), lit(0)))
    }

    def processDepartmentL2(df: DataFrame): DataFrame = {
      // Join df with orgHierarchyCompleteDF to get organisationID and remove duplicates
      val organisationDF = df
        .join(orgHierarchyCompleteDF, df("departmentMapID") === orgHierarchyCompleteDF("l2mapid"), "left")
        .select(df("departmentID"), col("sborgid").alias("organisationID")).dropDuplicates()

      // Aggregate organisation IDs and create allIDs column
      val sumDF = organisationDF
        .groupBy("departmentID")
        .agg(
          concat_ws(",", collect_set(col("organisationID"))).alias("orgIDs")
        )
        .withColumn("associatedIds", col("orgIDs"))
        .withColumn("allIDs", concat_ws(",", col("departmentID"), col("associatedIds")))

      // Process user learning hours
      val userJoinedDF = sumDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(userSumDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("departmentID", "allIDs")
        .agg(sum(col("totalLearningHours")).alias("learningSumValue"))

      // Process user login percentage
      val loginJoinedDF = userJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(userLoginPercentDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("departmentID", "allIDs", "learningSumValue")
        .agg(sum(col("totalLearningHours")).alias("loginSumValue"))

      // Process certificate data
      val certJoinedDF = loginJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(certSumDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("departmentID", "allIDs", "learningSumValue", "loginSumValue")
        .agg(sum(col("totalLearningHours")).alias("certSumValue"))

      // Process enrolment data
      val finalResultDF = certJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(enrolmentDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("departmentID", "allIDs", "learningSumValue", "loginSumValue", "certSumValue")
        .agg(sum(col("totalLearningHours")).alias("enrolmentSumValue"))
        .select(
          col("departmentID").alias("ministryID"),
          col("allIDs"),
          col("learningSumValue"),
          col("loginSumValue"),
          col("certSumValue"),
          col("enrolmentSumValue")
        )

      // Show and return final result with type casting and null handling
      show(finalResultDF, "finalresult")

      finalResultDF
        .withColumn("learningSumValue", coalesce(col("learningSumValue").cast("int"), lit(0)))
        .withColumn("loginSumValue", coalesce(col("loginSumValue").cast("int"), lit(0)))
        .withColumn("certSumValue", coalesce(col("certSumValue").cast("int"), lit(0)))
        .withColumn("enrolmentSumValue", coalesce(col("enrolmentSumValue").cast("int"), lit(0)))
    }


    def processMinistryL1(df: DataFrame): DataFrame = {

      println("Processing Ministry L1 DataFrame:")

      // Step 1: Create departmentAndMapIDsDF
      val departmentAndMapIDsDF = df
        .join(orgHierarchyCompleteDF, df("ministryMapID") === orgHierarchyCompleteDF("l1mapid"), "left")
        .select(df("ministryID"), col("sborgid").alias("departmentID"), col("mapid").alias("departmentMapID"))

      // Step 2: Create organisationDF
      val organisationDF = departmentAndMapIDsDF
        .join(orgHierarchyCompleteDF, departmentAndMapIDsDF("departmentMapID") === orgHierarchyCompleteDF("l2mapid"), "left")
        .select(departmentAndMapIDsDF("ministryID"), departmentAndMapIDsDF("departmentID"), col("sborgid").alias("organisationID")).dropDuplicates()
      show(organisationDF, "hierarchyF")

      // Step 3: Aggregate IDs
      val sumDF = organisationDF
        .groupBy("ministryID")
        .agg(
          concat_ws(",", collect_set(col("departmentID"))).alias("departmentIDs"),
          concat_ws(",", collect_set(col("organisationID"))).alias("orgIDs")
        )
        .withColumn("associatedIds", concat_ws(",", col("departmentIDs"), col("orgIDs")))
        .withColumn("allIDs", concat_ws(",", col("ministryID"), col("associatedIds")))

      // Step 4: Process learningSumValue
      val userJoinedDF = sumDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(userSumDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("ministryID", "allIDs")
        .agg(sum(col("totalLearningHours")).alias("learningSumValue"))

      // Step 5: Process loginSumValue
      val loginJoinedDF = userJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(userLoginPercentDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("ministryID", "allIDs", "learningSumValue")
        .agg(sum(col("totalLearningHours")).alias("loginSumValue"))

      // Step 6: Process certSumValue
      val certJoinedDF = loginJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(certSumDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("ministryID", "allIDs", "learningSumValue", "loginSumValue")
        .agg(sum(col("totalLearningHours")).alias("certSumValue"))

      // Step 7: Process enrolmentSumValue
      val finalResultDF = certJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(enrolmentDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("ministryID", "allIDs", "learningSumValue", "loginSumValue", "certSumValue")
        .agg(sum(col("totalLearningHours")).alias("enrolmentSumValue"))
        .select(
          col("ministryID"),
          col("allIDs"),
          col("learningSumValue"),
          col("loginSumValue"),
          col("certSumValue"),
          col("enrolmentSumValue")
        )

      show(finalResultDF, "finalresult")

      // Final type casting and null handling
      finalResultDF
        .withColumn("learningSumValue", coalesce(col("learningSumValue").cast("int"), lit(0)))
        .withColumn("loginSumValue", coalesce(col("loginSumValue").cast("int"), lit(0)))
        .withColumn("certSumValue", coalesce(col("certSumValue").cast("int"), lit(0)))
        .withColumn("enrolmentSumValue", coalesce(col("enrolmentSumValue").cast("int"), lit(0)))
    }

    // Create DataFrames based on conditions
    val ministryL1DF = joinedDF.filter(col("sborgtype") === "ministry" || col("sborgtype") === "state").select(col("sborgid").alias("ministryID"), col("mapid").alias("ministryMapID"))
    val ministryOrgDF = processMinistryL1(ministryL1DF)

    val departmentL2DF = joinedDF.filter(col("sborgtype") === "department" || col("sborgsubtype") === "department").select(col("sborgid").alias("departmentID"), col("mapid").alias("departmentMapID"))
    val deptOrgDF = processDepartmentL2(departmentL2DF)

    val orgsL3DF = joinedDF.filter(col("sborgtype") === "mdo" && col("sborgsubtype") =!= "department").select(col("sborgid").alias("organisationID"))
    val orgsDF = processOrgsL3(orgsL3DF)

    val combinedMinistryMetricsDF = ministryOrgDF.union(deptOrgDF).union(orgsDF)
    show(combinedMinistryMetricsDF, "MinistryMetrics")

    Redis.dispatchDataFrame[Int]("dashboard_rolled_up_user_count", combinedMinistryMetricsDF, "ministryID", "learningSumValue")
    Redis.dispatchDataFrame[Int]("dashboard_rolled_up_login_percent_last_24_hrs", combinedMinistryMetricsDF, "ministryID", "loginSumValue")
    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_certificates_generated_count", combinedMinistryMetricsDF, "ministryID", "certSumValue")
    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_enrolment_content_count", combinedMinistryMetricsDF, "ministryID", "enrolmentSumValue")
 }
}
