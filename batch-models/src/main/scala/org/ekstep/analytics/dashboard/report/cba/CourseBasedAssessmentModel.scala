package org.ekstep.analytics.dashboard.report.cba

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

import java.io.Serializable

object CourseBasedAssessmentModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.cba.CourseBasedAssessmentModel"
  override def name() = "CourseBasedAssessmentModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCourseBasedAssessmentData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }


  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   */
  def processCourseBasedAssessmentData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // get course details, with rating info
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF, runValidation = false, onlyCourse = true)

    val assessmentDF = assessmentESDataFrame(isAllAssess2 = true)
    val assessWithHierarchyDF = assessWithHierarchyDataFrame(assessmentDF, hierarchyDF, orgDF)
    val assessWithDetailsDF = assessWithHierarchyDF.drop("children")

    val assessChildrenDF = assessmentChildrenDataFrame(assessWithHierarchyDF)
    val userAssessmentDF = userAssessmentDataFrame()
    val userAssessChildrenDF = userAssessmentChildrenDataFrame(userAssessmentDF, assessChildrenDF)
    val userAssessChildrenDetailsDF = userAssessmentChildrenDetailsDataFrame(userAssessChildrenDF, assessWithDetailsDF,
      allCourseProgramDetailsWithRatingDF, userOrgDF)

    val orgHierarchyData = orgHierarchyDataframe()

    val userProfileDetails = userProfileDetailsDF(orgDF).select(
      col("profileDetails"), col("additionalProperties"), col("personalDetails"), col("professionalDetails"), col("userID"))

    var df = userAssessChildrenDetailsDF
      .join(userProfileDetails, Seq("userID"), "inner")
      .join(orgHierarchyData, Seq("userOrgName"), "left")

    df = df.withColumn("userAssessmentDuration", (unix_timestamp(col("assessEndTimestamp")) - unix_timestamp(col("assessStartTimestamp"))))
    show(df, "df 0")

    val latest = df
      .groupBy(col("assessChildID"), col("userID"))
      .agg(max("assessEndTimestamp").alias("assessEndTimestamp"))
    show(latest, "latest")

    df = df.join(latest, Seq("assessChildID", "userID", "assessEndTimestamp"), "inner")
    show(df, "df 1")

    df = df.withColumn("actualDuration", format_string("%02d:%02d:%02d", expr("userAssessmentDuration / 3600").cast("int"),
      expr("userAssessmentDuration % 3600 / 60").cast("int"),
      expr("userAssessmentDuration % 60").cast("int")
    ))
    show(df, "df 2")

    df = df.withColumn("totalAssessmentDuration", format_string("%02d:%02d:%02d", expr("assessExpectedDuration / 3600").cast("int"),
      expr("assessExpectedDuration % 3600 / 60").cast("int"),
      expr("assessExpectedDuration % 60").cast("int")
    ))
    show(df, "df 3")

    val caseExpression = "CASE WHEN assessPass == 1 THEN 'Yes' ELSE 'No' END"
    df = df.withColumn("Pass / Fail", expr(caseExpression))
    show(df, "df 4")

    val retakes = df.groupBy("assessChildID", "userID").agg(
      countDistinct("assessStartTime").alias("retakes"))
    df = df.join(retakes, Seq("assessChildID", "userID"), "left")
    show(df, "df 5")

    df = df.dropDuplicates("userID", "assessChildID")
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .select(
        col("userID"),
        col("assessID"),
        col("assessOrgID"),
        col("assessChildID"),
        col("userOrgID"),
        col("fullName").alias("Full Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("E mail"),
        col("personalDetails.mobile").alias("Phone Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organisation"),
        col("assessChildName").alias("Assessment Name"),
        col("assessPrimaryCategory").alias("Assessment Type"),
        col("assessOrgName").alias("Assessment Provider"),
        col("assessName").alias("Course Name"),
        col("totalAssessmentDuration").alias("Assessment Duration"),
        col("actualDuration").alias("Time spent by the user"),
        from_unixtime(col("assessEndTime"), "dd/MM/yyyy").alias("Completion Date"),
        col("assessResult").alias("Score Achieved"),
        col("assessOverallResult").alias("Overall Score"),
        col("assessPassPercentage").alias("Cut off Percentage"),
        col("Pass / Fail"),
        col("assessTotalQuestions").alias("Total Questions"),
        col("assessIncorrect").alias("No.of incorrect responses"),
        col("retakes").alias("No.of retakes"),
        col("userOrgID").alias("mdoid")
      )
    show(df, "final")

    df = df.coalesce(1)
    val reportPath = s"${conf.cbaReportPath}/${today}"
    generateFullReport(df, reportPath)
    df = df.drop("assessID", "assessOrgID", "assessChildID", "userOrgID")
    generateAndSyncReports(df, "mdoid", reportPath, "CBAssessmentReport")

    closeRedisConnect()

  }

}
