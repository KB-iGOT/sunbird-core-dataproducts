package org.ekstep.analytics.dashboard.report.assess

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object UserAssessmentModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.assess.UserAssessmentModel"
  override def name() = "UserAssessmentModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    try{
    val today = getDate()

    // obtain user org data
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    // get course details, with rating info
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
      allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF)

    val assessmentDF = assessmentESDataFrame(Seq("Standalone Assessment"))
    val assessWithHierarchyDF = assessWithHierarchyDataFrame(assessmentDF, hierarchyDF, orgDF)
    val assessWithDetailsDF = assessWithHierarchyDF.drop("children")

    // kafka dispatch to dashboard.assessment
    kafkaDispatch(withTimestamp(assessWithDetailsDF, timestamp), conf.assessmentTopic)

    val assessChildrenDF = assessmentChildrenDataFrame(assessWithHierarchyDF)
    
    val userAssessmentDF = cache.load("userAssessment")
    
    val userAssessChildrenDF = userAssessmentChildrenDataFrame(userAssessmentDF, assessChildrenDF)
    
    val userAssessChildrenDetailsDF = userAssessmentChildrenDetailsDataFrame(userAssessChildrenDF, assessWithDetailsDF,
      allCourseProgramDetailsWithRatingDF, userOrgDF)
    
    // kafka dispatch to dashboard.user.assessment
    kafkaDispatch(withTimestamp(userAssessChildrenDetailsDF, timestamp), conf.userAssessmentTopic)

    // get the mdoids for which the report are requesting
    // val mdoID = conf.mdoIDs
    // val mdoIDDF = mdoIDsDF(mdoID)

    // val mdoData = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("assessOrgID"), col("orgName"))
    // df = df.join(mdoData, Seq("assessOrgID"), "inner")

    val latest = userAssessChildrenDetailsDF.groupBy(col("assessChildID"), col("userID"))
      .agg(
        max("assessEndTimestamp").alias("assessEndTimestamp"),
        expr("COUNT(*)").alias("noOfAttempts")
      )

    val caseExpression = "CASE WHEN assessPass == 1 AND assessUserStatus == 'SUBMITTED' THEN 'Pass' WHEN assessPass == 0 AND assessUserStatus == 'SUBMITTED' THEN 'Fail' " +
      " ELSE 'N/A' END"
    val caseExpressionCompletionStatus = "CASE WHEN assessUserStatus == 'SUBMITTED' THEN 'Completed' ELSE 'In progress' END"

    val df = userAssessChildrenDetailsDF.join(broadcast(latest), Seq("assessChildID", "userID", "assessEndTimestamp"), "inner")
      .withColumn("Assessment_Status", expr(caseExpression))
      .withColumn("Overall_Status", expr(caseExpressionCompletionStatus))
      .withColumn("Report_Last_Generated_On", currentDateTime)
      .dropDuplicates("userID", "assessID")
      .select(
        col("userID").alias("User_ID"),
        col("fullName").alias("Full_Name"),
        col("assessName").alias("Assessment_Name"),
        col("assessLanguage").alias("Assessment_Language"),
        col("Overall_Status"),
        col("Assessment_Status"),
        col("assessPassPercentage").alias("Percentage_Of_Score"),
        col("noOfAttempts").alias("Number_of_Attempts"),
        col("maskedEmail").alias("Email"),
        col("userStatus").alias("status"),
        col("maskedPhone").alias("Phone"),
        col("assessOrgID").alias("mdoid"),
        col("Report_Last_Generated_On")
      ).coalesce(1)
    
    val columnsToKeepInReport = df.columns.filter(_ != "status")
    val reportPath = s"${conf.standaloneAssessmentReportPath}/${today}"
    // generateReport(df, s"${reportPath}-full")
    generateAndSyncReports(df.filter(col("status").cast("int") === 1).select(columnsToKeepInReport.map(col): _*), "mdoid",reportPath, "StandaloneAssessmentReport")

    Redis.closeRedisConnect()
  }catch {
    case e: Exception =>
      println(s"Error occurred during UserAssessmentModel processing: ${e.getMessage}", e)
      System.exit(1)
  }
  }

}
