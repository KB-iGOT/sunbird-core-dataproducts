package org.ekstep.analytics.dashboard.report.user

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._


object UserReportModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.user.UserReportModel"
  override def name() = "UserReportModel"
  /**
   * Pre processing steps before running the algorithm. Few pre-process steps are
   * 1. Transforming input - Filter/Map etc.
   * 2. Join/fetch data from LP
   * 3. Join/Fetch data from Cassandra
   */
  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  /**
   * Method which runs the actual algorithm
   */
  override def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = events.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processUserReport(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  /**
   * Post processing on the algorithm output. Some of the post processing steps are
   * 1. Saving data to Cassandra
   * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
   * 3. Transform into a structure that can be input to another data product
   */
  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processUserReport(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    // get user roles data
    val userRolesDF = roleDataFrame().groupBy("userID").agg(concat_ws(", ", collect_list("role")).alias("role"))    // return - userID, role

    val orgDF = orgDataFrame()
    val userDataDF = userProfileDetailsDF(orgDF)

    val orgHierarchyData = orgHierarchyDataframe()

    // get the mdoids for which the report are requesting
    // val mdoID = conf.mdoIDs
    // val mdoIDDF = mdoIDsDF(mdoID)

    // var df = mdoIDDF.join(orgDF, Seq("orgID"), "inner").select(col("orgID").alias("userOrgID"), col("orgName"))

    var df = userDataDF
      // .join(userDataDF, Seq("userOrgID"), "inner")
      .join(userRolesDF, Seq("userID"), "left")
      .join(orgHierarchyData, Seq("userOrgName"),"left")

    df = df.where(expr("userStatus=1"))
    df = df.withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))

    df = df.dropDuplicates("userID")
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .select(
        col("userID"),
        col("userOrgID"),
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        from_unixtime(col("userCreatedTimestamp"),"dd/MM/yyyy").alias("User_Registration_Date"),
        col("role").alias("Roles"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("userOrgID").alias("mdoid"),
        col("Report_Last_Generated_On")
      )

    df = df.coalesce(1)
    val reportPath = s"${conf.userReportPath}/${today}"
    generateFullReport(df, reportPath)
    df = df.drop("userID", "userOrgID")
    // generateReports(df, "mdoid", s"/tmp/${reportPath}", "UserReport")
    generateAndSyncReports(df, "mdoid", reportPath, "UserReport")

    closeRedisConnect()
  }
}
