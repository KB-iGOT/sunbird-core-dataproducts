package org.ekstep.analytics.dashboard.report.acbp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object UserACBPReportModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.acbp.UserACBPReportModel"
  override def name() = "UserACBPReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    try{
    val today = getDate()

    // get user and org data frames
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()
    val userDataDF = userOrgDF
      .join(orgHierarchyData, Seq("userOrgID"), "left")
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))
      .withColumn("group", coalesce(col("professionalDetails.group"), lit("")))
      .withColumn("userPrimaryEmail", col("personalDetails.primaryEmail"))
      .withColumn("userMobile", col("personalDetails.mobile"))
      .select("userID", "fullName", "userStatus", "userPrimaryEmail", "userMobile", "userOrgID", "ministry_name", "dept_name", "userOrgName", "designation", "group")

    // get course details and course enrolment data frames
    val hierarchyDF = contentHierarchyDataFrame()
    val allCourseProgramESDF = allCourseProgramESDataFrame(Seq("Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"))
    val allCourseProgramDetailsWithCompDF = allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramESDF, hierarchyDF, orgDF)
    val allCourseProgramDetailsDF = allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF)
    val userCourseProgramEnrolmentDF = userCourseProgramCompletionDataFrame()

    // get ACBP details data frame
    val acbpDF = acbpDetailsDF()

    val selectColumns = Seq("userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID", "ministry_name", "dept_name", "userOrgName", "userStatus", "acbpID",
      "assignmentType", "completionDueDate", "allocatedOn", "acbpCourseIDList","acbpStatus", "acbpCreatedBy","cbPlanName")
    val acbpAllotmentDF = explodedACBPDetails(acbpDF, userDataDF, selectColumns)

    // replace content list with names of the courses instead of ids
    val acbpAllEnrolmentDF = acbpAllotmentDF
      .withColumn("courseID", explode(col("acbpCourseIDList")))
      .join(allCourseProgramDetailsDF, Seq("courseID"), "left")
      .join(userCourseProgramEnrolmentDF, Seq("courseID", "userID"), "left")
      .na.drop(Seq("userID", "courseID"))
      .drop("acbpCourseIDList")

    // get cbplan data for warehouse
    val cbPlanWarehouseDF = acbpAllEnrolmentDF
      .withColumn("allotment_to", expr("CASE WHEN assignmentType='CustomUser' THEN userID WHEN assignmentType= 'Designation' THEN designation WHEN assignmentType = 'AllUser' THEN 'All Users' ELSE 'No Records' END"))
      .withColumn("data_last_generated_on", currentDateTime)
      .select(
        col("userOrgID").alias("org_id"),
        col("acbpCreatedBy").alias("created_by"),
        col("acbpID").alias("cb_plan_id"),
        col("cbPlanName").alias("plan_name"),
        col("assignmentType").alias("allotment_type"),
        col("allotment_to"),
        col("courseID").alias("content_id"),
        date_format(col("allocatedOn"), dateTimeFormat).alias("allocated_on"),
        date_format(col("completionDueDate"), dateFormat).alias("due_by"),
        //col("allocatedOn").alias("allocated_on"),
        //col("completionDueDate").alias("due_by"),
        col("acbpStatus").alias("status"),
        col("data_last_generated_on")
      )
      .distinct().orderBy("org_id","created_by","cbPlanName")

    // for particular userID and course ID, choose allotment entries based on priority rules
    val acbpEnrolmentDF = acbpAllEnrolmentDF.where(col("acbpStatus") === "Live")
      .groupByLimit(Seq("userID", "courseID"), "completionDueDate", 1, desc = true)
    kafkaDispatch(withTimestamp(acbpEnrolmentDF, timestamp), conf.acbpEnrolmentTopic)

    acbpEnrolmentDF.cache()

    // for enrolment report
    val enrolmentReportDataDF = acbpEnrolmentDF
      .withColumn("currentProgress", expr("CASE WHEN dbCompletionStatus=2 THEN 'Completed' WHEN dbCompletionStatus=1 THEN 'In Progress' WHEN dbCompletionStatus=0 THEN 'Not Started' ELSE 'Not Enrolled' END"))
      .withColumn("courseCompletedTimestamp",  date_format(col("courseCompletedTimestamp"), dateFormat))
      .withColumn("allocatedOn",  date_format(col("allocatedOn"), dateFormat))
      .withColumn("completionDueDate",  date_format(col("completionDueDate"), dateFormat))
      .na.fill("")

    val enrolmentReportDF = enrolmentReportDataDF
      .withColumn("MDO_Name", col("userOrgName"))
      .withColumn("Ministry", when(col("ministry_name").isNull, col("userOrgName")).otherwise(col("ministry_name")))
      .withColumn("Department", when(col("Ministry").isNotNull && col("Ministry") =!=  col("userOrgName") && (col("dept_name").isNull || col("dept_name") === ""), col("userOrgName")).otherwise(col("dept_name")))
      .withColumn("Organization",when(col("Ministry") =!=  col("userOrgName") && col("Department") =!= col("userOrgName"), col("userOrgName")).otherwise(lit("")))
      .select(
        col("fullName").alias("Name"),
        col("userPrimaryEmail").alias("Email"),
        col("userMobile").alias("Phone"),
        col("MDO_Name"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        col("Ministry"),
        col("Department"),
        col("Organization"),
        col("courseName").alias("Name of CBP Allocated Course"),
        col("allocatedOn").alias("Allocated On"),
        col("currentProgress").alias("Current Progress"),
        col("completionDueDate").alias("Due Date of Completion"),
        col("courseCompletedTimestamp").alias("Actual Date of Completion"),
        col("userStatus").alias("status"),
        col("userOrgID").alias("mdoid")
      ).withColumn("Report_Last_Generated_On", currentDateTime)
      .repartition(1)  // repartitioning here resolves a memory issue

    val columnsToKeepInEnrolmentReport = enrolmentReportDF.columns.filter(_ != "status")
    val reportPath = s"${conf.acbpReportPath}/${today}"
    generateReport(enrolmentReportDF.filter(col("status").cast("int") === 1).select(columnsToKeepInEnrolmentReport.map(col): _*).drop("mdoid").coalesce(1), s"${reportPath}/CBPEnrollmentReport", fileName="CBPEnrollmentReport")
    generateReport(enrolmentReportDF.filter(col("status").cast("int") === 1).select(columnsToKeepInEnrolmentReport.map(col): _*).coalesce(1),  s"${conf.acbpMdoEnrolmentReportPath}/${today}","mdoid", "CBPEnrollmentReport")
    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", s"${conf.acbpMdoEnrolmentReportPath}/${today}")
    }

    // for user summary report
    val userSummaryDataDF = acbpEnrolmentDF
      .withColumn("completionDueDateLong", expr("completionDueDate + INTERVAL 24 HOURS").cast(LongType))
      .withColumn("courseCompletedTimestampLong", col("courseCompletedTimestamp").cast(LongType))
      .groupBy("userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID", "ministry_name", "dept_name", "userOrgName", "userStatus")
      .agg(
        count("courseID").alias("allocatedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 AND courseCompletedTimestampLong<completionDueDateLong THEN 1 ELSE 0 END)").alias("completedBeforeDueDateCount")
      )

    val userSummaryReportDF = userSummaryDataDF
      .withColumn("MDO_Name", col("userOrgName"))
      .withColumn("Ministry", when(col("ministry_name").isNull, col("userOrgName")).otherwise(col("ministry_name")))
      .withColumn("Department", when(col("Ministry").isNotNull && col("Ministry") =!=  col("userOrgName") && (col("dept_name").isNull || col("dept_name") === ""), col("userOrgName")).otherwise(col("dept_name")))
      .withColumn("Organization",when(col("Ministry") =!=  col("userOrgName") && col("Department") =!= col("userOrgName"), col("userOrgName")).otherwise(lit("")))
      .select(
        col("fullName").alias("Name"),
        col("userPrimaryEmail").alias("Email"),
        col("userMobile").alias("Phone"),
        col("MDO_Name"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        col("Ministry"),
        col("Department"),
        col("Organization"),
        col("allocatedCount").alias("Number of CBP Courses Allocated"),
        col("completedCount").alias("Number of CBP Courses Completed"),
        col("completedBeforeDueDateCount").alias("Number of CBP Courses Completed within due date"),
        col("userStatus").alias("status"),
        col("userOrgID").alias("mdoid")
      ).withColumn("Report_Last_Generated_On", currentDateTime)
    val columnsToKeepInSummaryReport = userSummaryReportDF.columns.filter(_ != "status")
    generateReport(userSummaryReportDF.filter(col("status").cast("int") === 1).select(columnsToKeepInSummaryReport.map(col): _*).drop("mdoid").coalesce(1), s"${reportPath}/CBPUserSummaryReport", fileName="CBPUserSummaryReport")
    generateReport(userSummaryReportDF.filter(col("status").cast("int") === 1).select(columnsToKeepInSummaryReport.map(col): _*).coalesce(1), s"${conf.acbpMdoSummaryReportPath}/${today}","mdoid", "CBPUserSummaryReport")
    // to be removed once new security job is created
    if(conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", s"${conf.acbpMdoSummaryReportPath}/${today}")
    }
    syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)

    // changes for creating avro file for warehouse
    warehouseCache.write(cbPlanWarehouseDF.coalesce(1), conf.dwCBPlanTable)

    Redis.closeRedisConnect()
  }catch {
    case e: Exception =>
      // Log the error
      println(s"Error occurred during DataExhaustModel processing: ${e.getMessage}", e)
      // Exit with status 1
      System.exit(1)
  }
 }
}