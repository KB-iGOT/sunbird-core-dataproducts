package org.ekstep.analytics.dashboard.report.enrolment

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object UserEnrolmentModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.enrolment.UserEnrolmentModel"

  override def name() = "UserEnrolmentModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    try{
    val today = getDate()

    //GET ORG DATA
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()
    val userDataDF = userOrgDF
      .join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))

    // Get course data first
    val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"))

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()

    val userRatingDF = userCourseRatingDataframe()

    //use allCourseProgramDetailsDFWithOrgName below instead of allCourseProgramDetailsDF after adding orgname alias above
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userDataDF)

    val courseBatchDF = courseBatchDataFrame()
    val relevantBatchInfoDF = allCourseProgramDetailsDF.select("courseID", "category")
      .where(col("category").equalTo("Blended Program"))
      .join(courseBatchDF, Seq("courseID"), "left")
      .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")

    val ciosDataSchema = new StructType().add("content", new StructType()
      .add("name", StringType)
      .add("duration", StringType)
      .add("lastUpdatedOn", StringType)
      .add("contentPartner", new StructType()
        .add("id", StringType)
        .add("contentPartnerName", StringType)))

    val marketPlaceContentsDF = marketPlaceContentDF()
    val parsedDF = marketPlaceContentsDF.withColumn("parsed_data", from_json(col("cios_data"), ciosDataSchema))
    val marketPlaceEnrolmentsDF = marketPlaceEnrolments().withColumnRenamed("courseid", "content_id")
    // Extract the desired fields
    val extractedDF = parsedDF.select(col("content_id"),
      col("parsed_data.content.name").as("courseName"),
      col("parsed_data.content.duration").as("courseDuration"),
      col("parsed_data.content.lastUpdatedOn").as("courseLastPublishedOn"),
      col("parsed_data.content.contentPartner.id").as("courseOrgID"),
      col("parsed_data.content.contentPartner.contentPartnerName").as("courseOrgName"),
      lit("External Content").as("category"),
      lit("LIVE").as("courseStatus"))

    val marketPlaceContentEnrolmentsDF = extractedDF.durationFormat("courseDuration")
      .join(marketPlaceEnrolmentsDF, Seq("content_id"), "inner")
      .withColumn("courseCompletedTimestamp", date_format(col("completedon"), dateTimeFormat))
      .withColumn("courseEnrolledTimestamp", date_format(col("enrolled_date"), dateTimeFormat))
      .withColumn("lastContentAccessTimestamp", lit("Not Available"))
      .withColumn("userRating", lit("Not Available"))
      .withColumn("live_cbp_plan_mandate", lit(false))
      .withColumn("batchID", lit("Not Available"))
      .withColumn("issuedCertificateCount", size(col("issued_certificates")))
      .withColumn("certificate_generated", expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("certificateGeneratedOn", when(col("issued_certificates").isNull, "").otherwise( col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("lastIssuedOn")))
      .withColumn("firstCompletedOn", when(col("issued_certificates").isNull, "").otherwise(when(size(col("issued_certificates")) > 0, col("issued_certificates")(0).getItem("lastIssuedOn")).otherwise("")))
      .withColumn("certificateID", when(col("issued_certificates").isNull, "").otherwise( col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("identifier")))
      .withColumn("Report_Last_Generated_On", currentDateTime)
      .withColumnRenamed("userid", "userID")
      .withColumnRenamed("content_id", "courseID")
      .withColumnRenamed("progress", "courseProgress")
      .withColumnRenamed("status", "dbCompletionStatus")
      .na.fill(0, Seq("courseProgress", "issuedCertificateCount"))
      .na.fill("", Seq("certificateGeneratedOn"))

    val marketPlaceEnrolmentsWithUserDetailsDF = marketPlaceContentEnrolmentsDF.join(userDataDF, Seq("userID"), "left").withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
    val allCourseProgramCompletionWithDetailsWithBatchInfoDF = allCourseProgramCompletionWithDetailsDF.join(relevantBatchInfoDF, Seq("courseID", "batchID"), "left")

    val allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsWithBatchInfoDF.join(userRatingDF, Seq("courseID", "userID"), "left")

    val df = allCourseProgramCompletionWithDetailsDFWithRating
      .durationFormat("courseDuration")
      .withColumn("completedOn", date_format(col("courseCompletedTimestamp"), dateTimeFormat))
      .withColumn("enrolledOn", date_format(col("courseEnrolledTimestamp"), dateTimeFormat))
      .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"), dateTimeFormat))
      .withColumn("lastContentAccessTimestamp", date_format(col("lastContentAccessTimestamp"), dateTimeFormat))
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), dateFormat))
      .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), dateFormat))
      .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), dateFormat))
      .withColumn("completionPercentage", round(col("completionPercentage"), 2))
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .withColumn("Report_Last_Generated_On", currentDateTime)
      .withColumn("Certificate_Generated", expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("ArchivedOn", expr("CASE WHEN courseStatus == 'Retired' THEN lastStatusChangedOn ELSE '' END"))
      .withColumn("ArchivedOn", to_date(col("ArchivedOn"), dateFormat))
      .withColumn("Certificate_ID", col("certificateID"))
      .dropDuplicates("userID", "courseID", "batchID")

    allCourseProgramCompletionWithDetailsDFWithRating.persist(StorageLevel.MEMORY_ONLY)

    // read acbp data and filter the cbp plan based on status
    val acbpDF = acbpDetailsDF().where(col("acbpStatus") === "Live")

    val selectColumns = Seq("userID", "designation", "userOrgID", "acbpID", "assignmentType", "acbpCourseIDList","acbpStatus", "userStatus")
    val acbpAllotmentDF = explodedACBPDetails(acbpDF, userDataDF, selectColumns)

    // replace content list with names of the courses instead of ids
    val acbpAllEnrolmentDF = acbpAllotmentDF
      .withColumn("courseID", explode(col("acbpCourseIDList"))).withColumn("liveCBPlan", lit(true))
      .select(col("userOrgID"),col("courseID"),col("userID"),col("designation"),col("liveCBPlan"))

    val enrolmentWithACBP = df.join(acbpAllEnrolmentDF, Seq("userID", "userOrgID", "courseID"), "left")
      .withColumn("live_cbp_plan_mandate", when(col("liveCBPlan").isNull, false).otherwise(col("liveCBPlan")))

    val fullReportDF = enrolmentWithACBP
      .withColumn("MDO_Name", col("userOrgName"))
      .withColumn("Ministry", when(col("ministry_name").isNull, col("userOrgName")).otherwise(col("ministry_name")))
      .withColumn("Department", when(col("Ministry").isNotNull && col("Ministry") =!=  col("userOrgName") && (col("dept_name").isNull || col("dept_name") === ""), col("userOrgName")).otherwise(col("dept_name")))
      .withColumn("Organization",when(col("Ministry") =!=  col("userOrgName") && col("Department") =!= col("userOrgName"), col("userOrgName")).otherwise(lit("")))
      .select(
        col("userID"),
        col("userOrgID"),
        col("courseID"),
        col("courseOrgID"),
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("MDO_Name"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("Ministry"),
        col("Department"),
        col("Organization"),
        col("courseOrgName").alias("Content_Provider"),
        col("courseName").alias("Content_Name"),
        col("category").alias("Content_Type"),
        col("courseDuration").alias("Content_Duration"),
        col("batchID").alias("Batch_Id"),
        col("courseBatchName").alias("Batch_Name"),
        col("courseBatchStartDate").alias("Batch_Start_Date"),
        col("courseBatchEndDate").alias("Batch_End_Date"),
        col("enrolledOn").alias("Enrolled_On"),
        col("userCourseCompletionStatus").alias("Status"),
        col("completionPercentage").alias("Content_Progress_Percentage"),
        col("courseLastPublishedOn").alias("Last_Published_On"),
        col("ArchivedOn").alias("Content_Retired_On"),
        col("completedOn").alias("Completed_On"),
        col("Certificate_Generated"),
        col("userRating").alias("User_Rating"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("userOrgID").alias("mdoid"),
        col("issuedCertificateCount"),
        col("courseStatus"),
        col("courseResourceCount").alias("resourceCount"),
        col("courseProgress").alias("resourcesConsumed"),
        round(expr("CASE WHEN courseResourceCount=0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"), 2).alias("rawCompletionPercentage"),
        col("Certificate_ID"),
        col("Report_Last_Generated_On"),
        col("userStatus").alias("status"),
        col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate")
      ).dropDuplicates("userID","Batch_Id","courseID")
      .coalesce(1)

    val reportPath = s"${conf.userEnrolmentReportPath}/${today}"
    // generateReport(fullReportDF, s"${reportPath}-full")

    val mdoMarketplaceReport = marketPlaceEnrolmentsWithUserDetailsDF
      .withColumn("MDO_Name", col("userOrgName"))
      .withColumn("Ministry", when(col("ministry_name").isNull, col("userOrgName")).otherwise(col("ministry_name")))
      .withColumn("Department", when(col("Ministry").isNotNull && col("Ministry") =!=  col("userOrgName") && (col("dept_name").isNull || col("dept_name") === ""), col("userOrgName")).otherwise(col("dept_name")))
      .withColumn("Organization",when(col("Ministry") =!=  col("userOrgName") && col("Department") =!= col("userOrgName"), col("userOrgName")).otherwise(lit("")))
      .select(
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("MDO_Name"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("Ministry"),
        col("Department"),
        col("Organization"),
        col("courseOrgName").alias("Content_Provider"),
        col("courseName").alias("Content_Name"),
        col("category").alias("Content_Type"),
        col("courseDuration").alias("Content_Duration"),
        col("batchID").alias("Batch_Id"),
        lit("Not Available").alias("Batch_Name"),
        lit(null).cast("date").alias("Batch_Start_Date"),
        lit(null).cast("date").alias("Batch_End_Date"),
        col("courseEnrolledTimestamp").alias("Enrolled_On"),
        when(col("dbCompletionStatus").isNull, "not-enrolled")
          .when(col("dbCompletionStatus") === 0, "not-started")
          .when(col("dbCompletionStatus") === 1, "in-progress")
          .otherwise("completed")
          .alias("Status"),
        col("completionpercentage").alias("Content_Progress_Percentage"),
        to_date(col("courseLastPublishedOn"), dateFormat).alias("Last_Published_On"),
        lit(null).cast("date").alias("Content_Retired_On"),
        col("courseCompletedTimestamp").alias("Completed_On"),
        col("certificate_generated").alias("Certificate_Generated"),
        col("userRating").alias("User_Rating"),
        col("personalDetails.gender").alias("Gender"),
        lit("External Content").as("category"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("userOrgID").alias("mdoid"),
        col("certificateID").alias("Certificate_ID"),
        col("Report_Last_Generated_On"),
        col("userStatus"),
        col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate")
      )

    val mdoPlatformReport = fullReportDF.select(
      col("Full_Name"),col("Designation"),col("Email"),col("Phone_Number"),col("MDO_Name"),col("Group"),col("Tag"),col("Ministry"),col("Department"),
      col("Organization"),col("Content_Provider"),col("Content_Name"),col("Content_Type"),col("Content_Duration"),col("Batch_Id"),col("Batch_Name"),
      col("Batch_Start_Date"),col("Batch_End_Date"),col("Enrolled_On"),col("Status"),col("Content_Progress_Percentage"),col("Last_Published_On"),
      col("Content_Retired_On"),col("Completed_On"),col("Certificate_Generated"),col("User_Rating"),col("Gender"),col("Category"),col("External_System"),
      col("External_System_Id"),col("mdoid"),col("Certificate_ID"),col("Report_Last_Generated_On"),col("status"),col("Live_CBP_Plan_Mandate")
    )
    val mdoReportDF = mdoPlatformReport.union(mdoMarketplaceReport.withColumnRenamed("userStatus", "status"))
    val columnsToKeepInReport = mdoReportDF.columns.filter(_ != "status")
    //generateReport(mdoReportDF.filter(col("status") === 1).select(columnsToKeepInReport.map(col): _*).coalesce(1), reportPath, "mdoid","ConsumptionReport")
    generateReport(mdoReportDF.filter(col("status").cast("int") === 1).select(columnsToKeepInReport.map(col): _*).coalesce(1), reportPath, "mdoid","ConsumptionReport")
    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    }
    val marketPlaceWarehouseDF = marketPlaceEnrolmentsWithUserDetailsDF
      .withColumn("certificate_generated_on",date_format(from_utc_timestamp(to_utc_timestamp(to_timestamp(
        col("certificateGeneratedOn"), dateTimeWithMilliSecFormat), "UTC"), "IST"), dateTimeFormat))
      .withColumn("data_last_generated_on", currentDateTime)
      .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"), dateTimeFormat))
      .select(
        col("userID").alias("user_id"),
        col("batchID").alias("batch_id"),
        col("courseID").alias("content_id"),
        col("courseEnrolledTimestamp").alias("enrolled_on"),
        col("completionpercentage").alias("content_progress_percentage"),
        col("courseProgress").alias("resource_count_consumed"),
        when(col("dbCompletionStatus").isNull, "not-enrolled")
          .when(col("dbCompletionStatus") === 0, "not-started")
          .when(col("dbCompletionStatus") === 1, "in-progress")
          .otherwise("completed")
          .alias("user_consumption_status"),
        col("firstCompletedOn").alias("first_completed_on"),
        col("firstCompletedOn").alias("first_certificate_generated_on"),
        col("courseCompletedTimestamp").alias("last_completed_on"),
        col("certificate_generated_on").alias("last_certificate_generated_on"),
        col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
        col("certificate_generated").alias("certificate_generated"),
        col("issuedCertificateCount").alias("number_of_certificate"),
        col("userRating").alias("user_rating"),
        col("certificateID").alias("certificate_id"),
        col("live_cbp_plan_mandate"),
        col("data_last_generated_on")
      ).dropDuplicates("user_id","batch_id","content_id")

    val platformWarehouseDF = enrolmentWithACBP
      .withColumn("certificate_generated_on",date_format(from_utc_timestamp(to_utc_timestamp(to_timestamp(
        col("certificateGeneratedOn"), dateTimeWithMilliSecFormat), "UTC"), "IST"), dateTimeFormat))
      .withColumn("data_last_generated_on", currentDateTime)
      .select(
        col("userID").alias("user_id"),
        col("batchID").alias("batch_id"),
        col("courseID").alias("content_id"),
        col("enrolledOn").alias("enrolled_on"),
        col("completionPercentage").alias("content_progress_percentage"),
        col("courseProgress").alias("resource_count_consumed"),
        col("userCourseCompletionStatus").alias("user_consumption_status"),
        col("firstCompletedOn").alias("first_completed_on"),
        col("firstCompletedOn").alias("first_certificate_generated_on"),
        col("completedOn").alias("last_completed_on"),
        col("certificate_generated_on").alias("last_certificate_generated_on"),
        col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
        col("Certificate_Generated").alias("certificate_generated"),
        col("issuedCertificateCount").alias("number_of_certificate"),
        col("userRating").alias("user_rating"),
        col("Certificate_ID").alias("certificate_id"),
        col("live_cbp_plan_mandate"),
        col("data_last_generated_on")
      ).dropDuplicates("user_id","batch_id","content_id")

    val warehouseDF = platformWarehouseDF.union(marketPlaceWarehouseDF)

    val karmaPointsData = cache.load("userKarmaPoints")
      .select(col("userid").alias("user_id"),col("context_id").alias("content_id"),col("points"))
      .groupBy(col("user_id"), col("content_id")).agg(sum(col("points")).alias("karma_points"))

    val warehouseDFwithKarmaPoints = warehouseDF.join(karmaPointsData, Seq("user_id","content_id"), "left")
      .na.fill(0,Seq("karma_points"))
      .select(col("user_id"), col("batch_id"), col("content_id"), col("enrolled_on"), col("content_progress_percentage"),
        col("resource_count_consumed"), col("user_consumption_status"), col("first_completed_on"), col("first_certificate_generated_on"),
        col("last_completed_on"), col("last_certificate_generated_on"), col("content_last_accessed_on"), col("certificate_generated"),
        col("number_of_certificate"), col("user_rating"), col("certificate_id"), col("live_cbp_plan_mandate"), col("karma_points"),
        col("data_last_generated_on")
      )

    // changes for creating avro file for warehouse
    warehouseCache.write(warehouseDFwithKarmaPoints.coalesce(1), conf.dwEnrollmentsTable)

    allCourseProgramCompletionWithDetailsDFWithRating.unpersist()

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