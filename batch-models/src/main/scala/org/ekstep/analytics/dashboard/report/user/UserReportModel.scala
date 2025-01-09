package org.ekstep.analytics.dashboard.report.user

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object UserReportModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.user.UserReportModel"
  override def name() = "UserReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    // get user roles data
    val userRolesDF = roleDataFrame().groupBy("userID").agg(concat_ws(", ", collect_list("role")).alias("role")) // return - userID, role

    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val orgHierarchyData = orgHierarchyDataframe()
    val weeklyClapsDF = cache.load("weeklyClaps")
      .withColumnRenamed("userid", "userID")
      .withColumnRenamed("total_claps","weekly_claps_day_before_yesterday")
      .select(col("userID"), col("weekly_claps_day_before_yesterday"))
    val karmaPointsDF = cache.load("userKarmaPointsSummary")
      .withColumnRenamed("userid", "userID")
      .select(col("userID"), col("total_points"))

    val userEventDetailsDF = cache.load("eventEnrolmentDetails")
      .withColumnRenamed("user_id", "userID")
      .groupBy("userID")
      .agg(
        countDistinct(when(col("status").isin("not-started", "in-progress", "completed"), col("event_id"))).alias("total_event_enrolments"),
        countDistinct(when(col("status").equalTo("completed"), col("event_id"))).alias("total_event_completions"),
        sum(when(col("status").equalTo("completed") && col("certificate_id").isNotNull, col("event_duration_seconds"))).alias("total_event_learning_hours_with_certificates")
      ).withColumn("total_event_learning_hours_with_certificates", bround(col("total_event_learning_hours_with_certificates") / 3600, 2))

    val contentDurationDF = allCourseProgramESDataFrame(Seq("Course"))
      .select(col("courseID").alias("content_id"), col("courseDuration"), col("category"))
    val enrolmentsDataDF = warehouseCache.load("user_enrolments")
      .select(col("user_id").alias("userID"),col("content_id"),col("user_consumption_status"), col("certificate_id"))
      .join(contentDurationDF, Seq("content_id"), "left")
      .groupBy("userID")
      .agg(
        countDistinct(when(col("user_consumption_status").isin("not-started", "in-progress", "completed"), col("content_id"))).alias("total_content_enrolments"),
        countDistinct(when(col("user_consumption_status").equalTo("completed") && col("certificate_id").isNotNull, col("content_id"))).alias("total_content_completions"),
        sum(when(col("user_consumption_status").equalTo("completed") && col("certificate_id").isNotNull && col("category").equalTo("Course"), col("courseDuration"))).alias("total_content_duration")
      ).withColumn("total_content_duration", bround(col("total_content_duration") / 3600, 2))

    val userCompleteData = userOrgDF
      .join(userRolesDF, Seq("userID"), "left")
      .join(karmaPointsDF, Seq("userID"), "left")
      .join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")
      .join(weeklyClapsDF, Seq("userID"), "left")
      .join(userEventDetailsDF, Seq("userID"), "left")
      .join(enrolmentsDataDF, Seq("userID"), "left")
      .dropDuplicates("userID")
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .withColumn("Total_Learning_Hours", coalesce(col("total_event_learning_hours_with_certificates"), lit(0)) + coalesce(col("total_content_duration"), lit(0)))
      .withColumn("weekly_claps_day_before_yesterday", when(col("weekly_claps_day_before_yesterday").isNull || col("weekly_claps_day_before_yesterday") === "", 0).otherwise(col("weekly_claps_day_before_yesterday")))

    val mdoWiseReportDF = userCompleteData.filter(col("userStatus").cast("int") === 1)
      .withColumn("Report_Last_Generated_On", currentDateTime)
      .withColumn("Total_Enrolments", coalesce(col("total_event_enrolments"), lit(0)) + coalesce(col("total_content_enrolments"), lit(0)))
      .withColumn("Total_Completions", coalesce(col("total_event_completions"), lit(0)) + coalesce(col("total_content_completions"), lit(0)))
      .select(
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        from_unixtime(col("userCreatedTimestamp"), dateFormat).alias("User_Registration_Date"),
        col("role").alias("Roles"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("employmentDetails.employeeCode").alias("Employee_Id"),
        from_unixtime(col("userOrgCreatedDate"), dateFormat).alias("MDO_Created_On"),
        col("userProfileStatus").alias("Profile_Status"),
        col("weekly_claps_day_before_yesterday"),
        coalesce(col("total_points"), lit(0)).alias("Karma_Points"),
        coalesce(col("total_event_enrolments"), lit(0)).alias("Event_Enrolments"),
        coalesce(col("total_event_completions"), lit(0)).alias("Event_Completions"),
        coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("Event_Learning_Hours"),
        coalesce(col("total_content_enrolments"), lit(0)).alias("Course_Enrolments"),
        coalesce(col("total_content_completions"), lit(0)).alias("Course_Completions"),
        coalesce(col("total_content_duration"), lit(0)).alias("Course_Learning_Hours"),
        coalesce(col("Total_Enrolments"), lit(0)).alias("Total_Enrolments"),
        coalesce(col("Total_Completions"), lit(0)).alias("Total_Completions"),
        coalesce(col("Total_Learning_Hours"), lit(0)).alias("Total_Learning_Hours"),
        col("Report_Last_Generated_On"),
        col("userOrgID").alias("mdoid")
      ).coalesce(1)

    val reportPath = s"${conf.userReportPath}/${today}"
    generateReport(mdoWiseReportDF, reportPath, "mdoid", "UserReport")
//    if (conf.reportSyncEnable) {
//      syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
//    }

    val df_warehouse = userCompleteData
      .withColumn("marked_as_not_my_user", when(col("userProfileStatus") === "NOT-MY-USER", true).otherwise(false))
      .withColumn("data_last_generated_on", currentDateTime)
      .withColumn("is_verified_karmayogi", when(col("userProfileStatus") === "VERIFIED", true).otherwise(false))
      .select(
        col("userID").alias("user_id"),
        col("userOrgID").alias("mdo_id"),
        col("userStatus").alias("status"),
        coalesce(col("total_points"), lit(0)).alias("no_of_karma_points"),
        col("fullName").alias("full_name"),
        col("professionalDetails.designation").alias("designation"),
        col("personalDetails.primaryEmail").alias("email"),
        col("personalDetails.mobile").alias("phone_number"),
        col("professionalDetails.group").alias("groups"),
        col("Tag").alias("tag"),
        col("userProfileStatus").alias("profile_status"),
        date_format(from_unixtime(col("userCreatedTimestamp")), dateTimeFormat).alias("user_registration_date"),
        col("role").alias("roles"),
        col("personalDetails.gender").alias("gender"),
        col("personalDetails.category").alias("category"),
        col("marked_as_not_my_user"),
        col("is_verified_karmayogi"),
        col("userCreatedBy").alias("created_by_id"),
        col("additionalProperties.externalSystem").alias("external_system"),
        col("additionalProperties.externalSystemId").alias("external_system_id"),
        col("weekly_claps_day_before_yesterday"),
        coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("total_event_learning_hours"),
        coalesce(col("total_content_duration"), lit(0)).alias("total_content_learning_hours"),
        coalesce(col("Total_Learning_Hours"), lit(0)).alias("total_learning_hours"),
        col("employmentDetails.employeeCode").alias("employee_id"),
        col("data_last_generated_on")
      )
    // changes for creating avro file for warehouse
    warehouseCache.write(df_warehouse.coalesce(1), conf.dwUserTable)

    Redis.closeRedisConnect()
  }
}
