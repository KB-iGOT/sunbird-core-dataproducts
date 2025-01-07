package org.ekstep.analytics.dashboard.report.commsconsole

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object CommsReportModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.commsconsole.CommsReportModel"
  override def name() = "CommsReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val today = getDate()

    val dateFormat1 = "dd/MM/yyyy"
    val dateFormat2 = "yyyy-MM-dd"
    val bkEmailSuffix = conf.commsConsolePrarambhEmailSuffix
    val numOfDays = -conf.commsConsoleNumDaysToConsider
    val numOfTopLearners = conf.commsConsoleNumTopLearnersToConsider
    val currentDate = current_date()
    val dateNDaysAgo = date_add(currentDate, numOfDays)
    val lastUpdatedOn = date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a")
    val commsConsoleReportPath = s"${conf.commsConsoleReportPath}/${today}"

    val orgDF = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.orgHierarchyReportPath}/${today}-warehouse")
      .withColumn("department", when(col("ministry").isNotNull && col("department").isNull, col("mdo_name")).otherwise(col("department")))
      .withColumn("ministry", when(col("ministry").isNull && col("department").isNull, col("mdo_name")).otherwise(col("ministry")))
      .select("mdo_id", "ministry", "department", "organization")

    val userDF = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.userReportPath}/${today}-warehouse")
      //.withColumn("registrationDate", to_date(col("user_registration_date"), dateFormat1))
      .withColumn("registrationDate",  date_format(col("user_registration_date"), "dd/MM/yyyy HH:mm:ss a"))
      .select("user_id", "mdo_id", "status", "full_name", "email", "phone_number", "roles", "registrationDate", "tag", "user_registration_date")
      .join(orgDF, Seq("mdo_id"), "left")

    val rawEnrollmentsDF = spark.read.option("header", "true")
      .csv(s"${conf.localReportDir}/${conf.userEnrolmentReportPath}/${today}-warehouse")
      //.withColumn("completionDate", to_date(col("completed_on"), dateFormat2))
      .withColumn("completionDate",  date_format(col("content_last_accessed_on"), "dd/MM/yyyy HH:mm:ss a"))
    val enrollmentsDF = rawEnrollmentsDF
      .join(userDF, Seq("user_id"), "left")

    val mdoUserCountsDF = userDF.groupBy("mdo_id")
      .agg(
        count("*").alias("userCount")
      )

    val mdoCBPCompletionsDF = enrollmentsDF.filter(col("user_consumption_status") === "completed").groupBy("mdo_id")
      .agg(
        count("*").alias("completionCount")
      )
    val mdoAdminDF = userDF.filter(col("roles").contains("MDO_LEADER") || col("roles").contains("MDO_ADMIN"))
    val mdoCompletionRateDF = mdoCBPCompletionsDF.join(mdoUserCountsDF, Seq("mdo_id"), "left")
      .withColumn("completionPerUser", col("completionCount") / col("userCount"))
    val mdoCompletionRateWithAdminDetailsDF = mdoCompletionRateDF.join(mdoAdminDF, Seq("mdo_id"), "left")
      .orderBy(desc("completionPerUser"), desc("mdo_id"))
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("roles").alias("Role"),
        col("status"),
        col("userCount").alias("Total_Users"),
        col("completionCount").alias("Total_Content_Completion"),
        col("completionPerUser").alias("Content_Completion_Per_User"),
        col("Last_Updated_On")
      )
    val columnsToKeepInMDOCompletionRateReport = mdoCompletionRateWithAdminDetailsDF.columns.filter(_ != "status")
    generateReport(mdoCompletionRateWithAdminDetailsDF.filter(col("status").cast("int") === 1).select(columnsToKeepInMDOCompletionRateReport.map(col): _*), s"${commsConsoleReportPath}/AllMDOsContentCompletion", fileName="AllMDOsContentCompletion")

    val usersWithEnrollments = enrollmentsDF.select("user_id").distinct()
    //users who have not been enrolled in any cbp
    val usersWithoutAnyEnrollments = userDF.select("user_id").distinct().except(usersWithEnrollments)
    val usersWithoutAnyEnrollmentsWithUserDetailsDF = usersWithoutAnyEnrollments.join(userDF, Seq("user_id"), "left")
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("status"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("user_registration_date").alias("User_Registration_Date"),
        col("Last_Updated_On")
      )
    val columnsToKeepInUsersWithourEnrolmentsReport = usersWithoutAnyEnrollmentsWithUserDetailsDF.columns.filter(_ != "status")
    generateReport(usersWithoutAnyEnrollmentsWithUserDetailsDF.filter(col("status").cast("int") === 1).select(columnsToKeepInUsersWithourEnrolmentsReport.map(col): _*), s"${commsConsoleReportPath}/UsersOnboardedNotSignedUpAnyContent", fileName="UsersOnboardedNotSignedUpAnyContent")

    // users created in last 15 days, but not enrolled in any cbp
    val usersCreatedInLastNDaysDF = userDF.filter(col("user_registration_date").between(dateNDaysAgo, currentDate)).select("user_id").distinct()
    val usersCreatedInLastNDaysWithoutEnrollmentsDF = usersCreatedInLastNDaysDF.except(usersWithEnrollments)
    val usersCreatedInLastNDaysWithoutEnrollmentsWithUserDetailsDF = usersCreatedInLastNDaysWithoutEnrollmentsDF.join(userDF, Seq("user_id"), "left")
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("status"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("user_registration_date").alias("User_Registration_Date"),
        col("Last_Updated_On")
      )
    val columnsToKeepInUsersCreatedReport = usersCreatedInLastNDaysWithoutEnrollmentsWithUserDetailsDF.columns.filter(_ != "status")
    generateReport(usersCreatedInLastNDaysWithoutEnrollmentsWithUserDetailsDF.filter(col("status").cast("int") === 1).select(columnsToKeepInUsersCreatedReport.map(col): _*), s"${commsConsoleReportPath}/UsersOnboardedLast15DaysNotSignedUpAnyContent", fileName="UsersOnboardedLast15DaysNotSignedUpAnyContent")

    //top 60 users ranked by cbp completion in last 15 days
    val topXCompletionsInNDays = enrollmentsDF.filter(col("content_last_accessed_on").between(dateNDaysAgo, currentDate))
      .groupBy("user_id")
      .agg(
        count("*").alias("completionCount")
      )
      .orderBy(desc("completionCount")).limit(numOfTopLearners)
      .join(userDF, Seq("user_id"), "left")
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("status"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("user_registration_date").alias("User_Registration_Date"),
        col("completionCount").alias("Content_Completion"),
        col("Last_Updated_On")
      )
    val columnsToKeepInTopXCompletionReport = topXCompletionsInNDays.columns.filter(_ != "status")
    generateReport(topXCompletionsInNDays.filter(col("status").cast("int") === 1).select(columnsToKeepInTopXCompletionReport.map(col): _*), s"${commsConsoleReportPath}/Top1LakhUsersContentCompletionLast15Days", fileName="Top1LakhUsersContentCompletionLast15Days")

    val prarambhCourses = conf.commsConsolePrarambhCbpIds.split(",").map(_.trim).toList
    val rozgarTags =  conf.commsConsolePrarambhTags.split(",").map(_.trim).toList
    val checkForRozgarTag = rozgarTags.map(value => expr(s"lower(tag) like '%$value%'")).reduce(_ or _)
    val checkForKBEmail = expr(s"email LIKE '%$bkEmailSuffix'")
    val rozgarUsersDF = userDF.filter(checkForKBEmail || checkForRozgarTag)
    val prarambhCourseCount = prarambhCourses.size
    val prarambhCompletionCount = conf.commsConsolePrarambhNCount

    val prarambhEnrollments = rawEnrollmentsDF
      .filter(col("user_consumption_status") === "completed" && col("content_id").isin(prarambhCourses: _*))
    val prarambEnrollmentsByRozgarUsersDF = rozgarUsersDF.join(prarambhEnrollments, Seq("user_id"), "left")
    val prarambhCompletionCountsDF = prarambEnrollmentsByRozgarUsersDF.groupBy("user_id").agg(
      count("*").alias("prarambhCompletionCount"),
      max("completionDate").alias("Completed_On")
    )
    val prarambhUserDataWithCompletionCountsDF = prarambhCompletionCountsDF.join(rozgarUsersDF, Seq("user_id"), "inner")
      .withColumn("Last_Updated_On", lastUpdatedOn)
      .select(
        col("full_name").alias("Name"),
        col("email").alias("Email"),
        col("status"),
        col("phone_number").alias("Phone_Number"),
        col("ministry").alias("Ministry"),
        col("department").alias("Department"),
        col("organization").alias("Organization"),
        col("user_registration_date").alias("User_Registration_Date"),
        col("Completed_On"),
        col("Last_Updated_On"),
        col("prarambhCompletionCount")
      )

    val columnsToKeepInPrarambhCourseReport = prarambhUserDataWithCompletionCountsDF.columns.filter(_ != "status")
    generateReport(prarambhUserDataWithCompletionCountsDF.filter(col("prarambhCompletionCount") === prarambhCompletionCount).filter(col("status").cast("int") === 1).select(columnsToKeepInPrarambhCourseReport.map(col): _*)
      .drop("prarambhCompletionCount")
      , s"${commsConsoleReportPath}/UsersCompleted6PrarambhCoursesPendingFullCompletion", fileName="UsersCompleted6PrarambhCoursesPendingFullCompletion")
    generateReport(prarambhUserDataWithCompletionCountsDF.filter(col("prarambhCompletionCount") === prarambhCourseCount).filter(col("status").cast("int") === 1).select(columnsToKeepInPrarambhCourseReport.map(col): _*)
      .drop("prarambhCompletionCount")
      , s"${commsConsoleReportPath}/UsersFinishedEntirePrarambhModule", fileName="UsersFinishedEntirePrarambhModule")

    syncReports(s"${conf.localReportDir}/${commsConsoleReportPath}", commsConsoleReportPath)

    Redis.closeRedisConnect()

  }
}