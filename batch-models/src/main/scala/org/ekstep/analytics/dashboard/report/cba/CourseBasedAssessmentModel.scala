package org.ekstep.analytics.dashboard.report.cba

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object CourseBasedAssessmentModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.cba.CourseBasedAssessmentModel"
  override def name() = "CourseBasedAssessmentModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    try{
      val today = getDate()

    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    orgDF.cache()
    // get course details, with rating info
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Standalone Assessment", "Curated Program"), runValidation = false)

    val assessmentDF = assessmentESDataFrame(Seq("Course", "Standalone Assessment", "Blended Program"))
    val assessWithHierarchyDF = assessWithHierarchyDataFrame(assessmentDF, hierarchyDF, orgDF).cache()
    val assessWithDetailsDF = assessWithHierarchyDF.drop("children")

    val assessChildrenDF = assessmentChildrenDataFrame(assessWithHierarchyDF)
    val userAssessmentDF = cache.load("userAssessment").filter(col("assessUserStatus") === "SUBMITTED")
      .withColumn("assessStartTime", col("assessStartTimestamp").cast("long"))
      .withColumn("assessEndTime", col("assessEndTimestamp").cast("long"))
    val userAssessChildrenDF = userAssessmentChildrenDataFrame(userAssessmentDF, assessChildrenDF)
    val userAssessChildrenDetailsDF = userAssessmentChildrenDetailsDataFrame(userAssessChildrenDF, assessWithDetailsDF,
      allCourseProgramDetailsWithRatingDF, userOrgDF)

    val orgHierarchyData = orgHierarchyDataframe()
    val userAssessChildDataDF = userAssessChildrenDetailsDF
      .join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")

    val retakesDF = userAssessChildDataDF
      .groupBy("assessChildID", "userID")
      .agg(countDistinct("assessStartTime").alias("retakes"))

    val userAssessChildDataLatestDF = userAssessChildDataDF
      .groupByLimit(Seq("assessChildID", "userID"), "assessEndTimestamp", 1, desc = true)
      .drop("rowNum")
      .join(broadcast(retakesDF), Seq("assessChildID", "userID"), "left")

    val finalDF = userAssessChildDataLatestDF
      .withColumn("userAssessmentDuration", unix_timestamp(col("assessEndTimestamp")) - unix_timestamp(col("assessStartTimestamp")))
      .withColumn("Pass", when(col("assessPass") === 1, "Yes").otherwise("No"))
      .durationFormat("assessExpectedDuration", "totalAssessmentDuration")
      .withColumn("assessPercentage", when(col("assessPassPercentage").isNotNull, col("assessPassPercentage"))
        .otherwise(lit("Need to pass in all sections")))
      .withColumn("assessment_type", when(col("assessCategory") === "Standalone Assessment", col("assessCategory"))
        .when(col("assessPrimaryCategory").isNotNull, col("assessPrimaryCategory"))
        .otherwise(lit("")))
      .withColumn("assessment_course_name", when(col("assessment_type") === "Course Assessment", col("assessName"))
        .otherwise(lit("")))
      .withColumn("Total_Score_Calculated", when(col("assessMaxQuestions").isNotNull, col("assessMaxQuestions") * 1))
      .withColumn("course_id", when(col("assessCategory") === "Standalone Assessment", lit(""))
        .otherwise(col("assessID")))
      .withColumn("Tags", concat_ws(", ", col("additionalProperties.tag")))
      .cache()

    val fullReportDFNew = finalDF
      .withColumn("MDO_Name", col("userOrgName"))
      .withColumn("Ministry", when(col("ministry_name").isNull, col("userOrgName")).otherwise(col("ministry_name")))
      .withColumn("Department", when(col("Ministry").isNotNull && col("Ministry") =!=  col("userOrgName") && (col("dept_name").isNull || col("dept_name") === ""), col("userOrgName")).otherwise(col("dept_name")))
      .withColumn("Organization",when(col("Ministry") =!=  col("userOrgName") && col("Department") =!= col("userOrgName"), col("userOrgName")).otherwise(lit("")))
      .select(
        col("userID"),
        col("assessChildID").alias("assessment_id"),
        col("assessID"),
        col("assessOrgID"),
        col("userOrgID"),
        col("fullName"),
        col("userStatus").alias("status"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("E mail"),
        col("personalDetails.mobile").alias("Phone Number"),
        col("MDO_Name"),
        col("professionalDetails.group").alias("Group"),
        col("Tags"),
        col("Ministry"),
        col("Department"),
        col("Organisation"),
        col("assessChildName").alias("assessment_name"),
        col("assessment_type"),
        col("assessOrgName").alias("assessment_content_provider"),
        from_unixtime(col("assessLastPublishedOn").cast("long"), dateFormat).alias("assessment_publish_date"),
        col("assessment_course_name").alias("course_name"),
        col("course_id"),
        col("totalAssessmentDuration").alias("assessment_duration"),
        from_unixtime(col("assessEndTime"), dateFormat).alias("last_attempted_date"),
        col("assessOverallResult").alias("latest_percentage_achieved"),
        col("assessPercentage"),
        col("Pass"),
        col("assessMaxQuestions").alias("total_questions"),
        col("assessIncorrect").alias("incorrect_count"),
        col("assessBlank").alias("unattempted_questions"),
        col("retakes"),
        col("assessEndTime"),
        col("userOrgID").alias("mdoid")
      ).repartition(1)


    // Join using a single column or Seq if multiple columns
    val userOrgHierarchyDataDF = userOrgDF.join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")
    val oldAssessmentData = cache.load("oldAssessmentDetails").withColumnRenamed("user_id", "userID").withColumnRenamed("parent_source_id", "courseID")

    val fullReportDFOldDraft = oldAssessmentData.join(allCourseProgramDetailsDF, Seq("courseID"), "left").join(userOrgHierarchyDataDF, Seq("userID"), "left")
      .withColumn("assessment_type", lit("Learning Resource"))
      .withColumn("total_questions", col("correct_count") + col("incorrect_count") + col("not_answered_count"))
      .withColumn("assessment_publish_date", lit(null).cast("date"))
      .withColumn("assessment_duration", lit(null).cast("string"))
      .withColumn("last_attempted_date", lit(null).cast("date"))
      .withColumn("retakes", lit(null).cast("integer"))
      .withColumn("assessEndTime", lit(null).cast("bigint"))
      .withColumn("assessChildID", lit(null).cast("string"))
      .withColumn("Pass", when(col("result_percent") >= col("pass_percent"), lit("Yes")).otherwise(lit("No")))
      .withColumn("Tags", concat_ws(", ", col("additionalProperties.tag")))

    val fullReportDFOld = fullReportDFOldDraft
      .withColumn("MDO_Name", col("userOrgName"))
      .withColumn("Ministry", when(col("ministry_name").isNull, col("userOrgName")).otherwise(col("ministry_name")))
      .withColumn("Department", when(col("Ministry").isNotNull && col("Ministry") =!=  col("userOrgName") && (col("dept_name").isNull || col("dept_name") === ""), col("userOrgName")).otherwise(col("dept_name")))
      .withColumn("Organization",when(col("Ministry") =!=  col("userOrgName") && col("Department") =!= col("userOrgName"), col("userOrgName")).otherwise(lit("")))
      .select(
        col("userID"),
        col("source_id").alias("assessment_id"),
        col("courseOrgID"),
        col("assessChildID"),
        col("userOrgID"),
        col("fullName"),
        col("userStatus"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("E mail"),
        col("personalDetails.mobile").alias("Phone Number"),
        col("MDO_Name"),
        col("professionalDetails.group").alias("Group"),
        col("Tags"),
        col("Ministry"),
        col("Department"),
        col("Organisation"),
        col("source_title").alias("assessment_name"),
        col("assessment_type"),
        col("courseOrgID").alias("assessment_content_provider"),
        col("assessment_publish_date"),
        col("courseName").alias("course_name"),
        col("courseID").alias("course_id"),
        col("assessment_duration"),
        col("last_attempted_date"),
        col("result_percent").alias("latest_percentage_achieved"),
        col("pass_percent").alias("assessPercentage"),
        col("Pass"),
        col("total_questions"),
        col("incorrect_count"),
        col("not_answered_count").alias("unattempted_questions"),
        col("retakes"),
        col("assessEndTime"),
        col("userOrgID").alias("mdoid")
      )

    val fullReportDF = fullReportDFNew.union(fullReportDFOld)
      .withColumn("Report_Last_Generated_On", currentDateTime).dropDuplicates("userID", "assessment_id", "course_id")
    val reportPath = s"${conf.cbaReportPath}/${today}"
    // generateReport(fullReportDF, s"${reportPath}-full")

    val mdoReportDF = fullReportDF
      .select(
        col("userID").alias("User ID"),
        col("fullName").alias("Full Name"),
        col("Designation"),
        col("E mail"),
        col("Phone Number"),
        col("MDO_Name"),
        col("Group"),
        col("Tags"),
        col("status"),
        col("Ministry"),
        col("Department"),
        col("Organisation"),
        col("assessment_name").alias("Assessment Name"),
        col("assessment_type").alias("Assessment Type"),
        col("assessment_content_provider").alias("Assessment/Content Provider"),
        col("assessment_publish_date").alias("Assessment Publish Date"),
        col("course_name").alias("Course Name"),
        col("course_id").alias("Course ID"),
        col("assessment_duration").alias("Assessment Duration"),
        col("last_attempted_date").alias("Last Attempted Date"),
        col("latest_percentage_achieved").alias("Latest Percentage Achieved"),
        col("assessPercentage").alias("Cut off Percentage"),
        col("Pass"),
        col("total_questions").alias("Total Questions"),
        col("incorrect_count").alias("No of Incorrect Responses"),
        col("unattempted_questions").alias("Unattempted Questions"),
        col("retakes").alias("No of Retakes"),
        col("mdoid"),
        col("Report_Last_Generated_On")
      ).coalesce(1)
    val columnsToKeepInReport = mdoReportDF.columns.filter(_ != "status")
    generateReport(mdoReportDF.filter(col("status") === 1).select(columnsToKeepInReport.map(col): _*), reportPath,"mdoid", "UserAssessmentReport")

    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    }

    val warehouseDF = fullReportDF
      .withColumn("data_last_generated_on", currentDateTime)
      .select(
        col("userID").alias("user_id"),
        col("course_id").alias("content_id"),
        col("assessment_id"),
        col("assessment_name").alias("assessment_name"),
        col("assessment_type").alias("assessment_type"),
        col("assessment_duration").alias("assessment_duration"),
        col("assessment_duration").alias("time_spent_by_the_user"),
        //from_unixtime(col("assessEndTime"), "dd/MM/yyyy").alias("completion_date"),
        date_format(from_unixtime(col("assessEndTime")), dateTimeFormat).alias("completion_date"),
        col("latest_percentage_achieved").alias("score_achieved"),
        col("total_questions").alias("overall_score"),
        col("assessPercentage").alias("cut_off_percentage"),
        col("total_questions").alias("total_question"),
        col("incorrect_count").alias("number_of_incorrect_responses"),
        col("retakes").alias("number_of_retakes"),
        col("data_last_generated_on")
      )

    // changes for creating avro file for warehouse
    warehouseCache.write(warehouseDF.coalesce(1), conf.dwAssessmentTable)

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