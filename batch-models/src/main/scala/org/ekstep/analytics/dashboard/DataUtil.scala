package org.ekstep.analytics.dashboard

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct, explode_outer, expr, from_json, last, lit, lower, max, to_timestamp, udf}
import DashboardUtil._
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, BooleanType, FloatType, IntegerType, StringType, StructField, StructType}
import org.ekstep.analytics.framework.FrameworkContext

import java.io.Serializable
import java.util
import scala.util.matching.Regex


object DataUtil extends Serializable {

  def elasticSearchCourseProgramDataFrame(primaryCategories: Seq[String])(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val shouldClause = primaryCategories.map(pc => s"""{"match":{"primaryCategory.raw":"${pc}"}}""").mkString(",")
    val query = s"""{"_source":["identifier","name","primaryCategory","status","reviewStatus","channel","duration","leafNodesCount"],"query":{"bool":{"should":[${shouldClause}]}}}"""
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel", "duration", "leafNodesCount")
    elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields)
  }

  def fracCompetencyAPI(host: String): String = {
    val url = s"https://${host}/graphql"
    val requestBody = """{"operationName":"filterCompetencies","variables":{"cod":[],"competencyType":[],"competencyArea":[],"competencySector":[]},"query":"query filterCompetencies($cod: [String], $competencyType: [String], $competencyArea: [String], $competencySector: [String]) {\n  getAllCompetencies(\n    cod: $cod\n    competencyType: $competencyType\n    competencyArea: $competencyArea\n    competencySector: $competencySector\n  ) {\n    name\n    id\n    description\n    status\n    source\n    additionalProperties {\n      cod\n      competencyType\n      competencyArea\n      competencySector\n      __typename\n    }\n    __typename\n  }\n}\n"}"""
    api("POST", url, requestBody)
  }

  def fracCompetencyDFOption(host: String)(implicit spark: SparkSession): Option[DataFrame] = {
    var result = fracCompetencyAPI(host)
    result = result.trim()
    // return empty data frame if result is an empty string
    if (result == "") {
      println(s"ERROR: fracCompetencyAPI returned empty string")
      return None
    }
    val df = dataFrameFromJSONString(result)  // parse json string
    if (df.isEmpty) {
      println(s"ERROR: druidSQLAPI json parse result is empty")
      return None
    }
    // return empty data frame if there is an `errors` field in the json
    if (hasColumn(df, "errors")) {
      println(s"ERROR: fracCompetencyAPI returned error response, response=${result}")
      return None
    }
    // now that error handling is done, proceed with business as usual
    Some(df)
  }

  /**
   * completionPercentage   completionStatus    IDI status
   * NULL                   not-enrolled        not-started
   * 0.0                    enrolled            not-started
   * 0.0 < % < 10.0         started             enrolled
   * 10.0 <= % < 100.0      in-progress         in-progress
   * 100.0                  completed           completed
   * @param df data frame with completionPercentage column
   * @return df with completionStatus column
   */
  def withCompletionStatusColumn(df: DataFrame): DataFrame = {
    val caseExpression = "CASE WHEN ISNULL(completionPercentage) THEN 'not-enrolled' WHEN completionPercentage == 0.0 THEN 'enrolled' WHEN completionPercentage < 10.0 THEN 'started' WHEN completionPercentage < 100.0 THEN 'in-progress' ELSE 'completed' END"
    df.withColumn("completionStatus", expr(caseExpression))
  }


  /**
   * org data from cassandra
   * @return DataFrame(orgID, orgName, orgStatus)
   */
  def orgDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val orgDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOrgTable)
      .select(
        col("id").alias("orgID"),
        col("orgname").alias("orgName"),
        col("status").alias("orgStatus")
      ).na.fill("", Seq("orgName"))

    show(orgDF, "Org DataFrame")

    orgDF
  }

  /**
   * user data from cassandra
   * @return DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus, userCreatedTimestamp, userUpdatedTimestamp)
   */
  def userDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    // userID, orgID, userStatus
    var userDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)
      .select(
        col("id").alias("userID"),
        col("firstname").alias("firstName"),
        col("lastname").alias("lastName"),
        col("maskedemail").alias("maskedEmail"),
        col("rootorgid").alias("userOrgID"),
        col("status").alias("userStatus"),
        col("createddate").alias("userCreatedTimestamp"),
        col("updateddate").alias("userUpdatedTimestamp")
      ).na.fill("", Seq("userOrgID"))

    userDF = userDF
      .withColumn("userCreatedTimestamp", to_timestamp(col("userCreatedTimestamp"), "yyyy-MM-dd HH:mm:ss:SSSZ"))
      .withColumn("userCreatedTimestamp", col("userCreatedTimestamp").cast("long"))
      .withColumn("userUpdatedTimestamp", to_timestamp(col("userUpdatedTimestamp"), "yyyy-MM-dd HH:mm:ss:SSSZ"))
      .withColumn("userUpdatedTimestamp", col("userUpdatedTimestamp").cast("long"))

    show(userDF, "User DataFrame")

    userDF
  }


  /**
   *
   * @param orgDF DataFrame(orgID, orgName, orgStatus)
   * @param userDF DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus, userCreatedTimestamp, userUpdatedTimestamp)
   * @return DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userCreatedTimestamp, userUpdatedTimestamp, userOrgID, userOrgName, userOrgStatus)
   */
  def userOrgDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    val joinOrgDF = orgDF.select(
      col("orgID").alias("userOrgID"),
      col("orgName").alias("userOrgName"),
      col("orgStatus").alias("userOrgStatus")
    )
    val userInfoDF = userDF.join(joinOrgDF, Seq("userOrgID"), "left")
    show(userInfoDF, "User Info DataFrame")

    userInfoDF
  }

  /**
   *
   * @return DataFrame(userID, role)
   */
  def roleDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val roleDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserRolesTable)
      .select(
        col("userid").alias("userID"),
        col("role").alias("role")
      )
    show(roleDF, "User Role DataFrame")

    roleDF
  }

  /**
   *
   * @param userOrgDF DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus)
   * @param roleDF DataFrame(userID, role)
   * @return DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   */
  def userOrgRoleDataFrame(userOrgDF: DataFrame, roleDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    // userID, userStatus, orgID, orgName, orgStatus, role
    val joinUserOrgDF = userOrgDF.select(
      col("userID"), col("userStatus"),
      col("userOrgID"), col("userOrgName"), col("userOrgStatus")
    )
    val userOrgRoleDF = joinUserOrgDF.join(roleDF, Seq("userID"), "left").where(expr("userStatus=1 AND userOrgStatus=1"))
    show(userOrgRoleDF)

    userOrgRoleDF
  }

  /**
   *
   * @param userOrgRoleDF DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   * @return DataFrame(role, count)
   */
  def roleCountDataFrame(userOrgRoleDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val roleCountDF = userOrgRoleDF.groupBy("role").agg(countDistinct("userID").alias("count"))
    show(roleCountDF)

    roleCountDF
  }

  /**
   *
   * @param userOrgRoleDF DataFrame(userID, userStatus, userOrgID, userOrgName, userOrgStatus, role)
   * @return DataFrame(orgID, orgName, role, count)
   */
  def orgRoleCountDataFrame(userOrgRoleDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val orgRoleCount = userOrgRoleDF.groupBy("userOrgID", "role").agg(
      last("userOrgName").alias("orgName"),
      countDistinct("userID").alias("count")
    ).select(
      col("userOrgID").alias("orgID"),
      col("orgName"), col("role"), col("count")
    )
    show(orgRoleCount)

    orgRoleCount
  }

  /**
   *
   * @param orgDF DataFrame(orgID, orgName, orgStatus)
   * @param userDF DataFrame(userID, firstName, lastName, maskedEmail, userOrgID, userStatus)
   * @return DataFrame(orgID, orgName, registeredCount, totalCount)
   */
  def orgUserCountDataFrame(orgDF: DataFrame, userDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val orgUserDF = orgDF.join(userDF.withColumnRenamed("userOrgID", "orgID").filter(col("orgID").isNotNull), Seq("orgID"), "left")
      .where(expr("userStatus=1 AND orgStatus=1"))
    show(orgUserDF, "Org User DataFrame")

    val orgUserCountDF = orgUserDF.groupBy("orgID", "orgName").agg(expr("count(userID)").alias("registeredCount"))
      .withColumn("totalCount", lit(10000))
    show(orgUserCountDF, "Org User Count DataFrame")

    orgUserCountDF
  }

  /**
   *
   * @param orgUserCountDF DataFrame(orgID, orgName, registeredCount, totalCount)
   * @return registered user count map, total  user count map, and orgID-orgName map
   */
  def getOrgUserMaps(orgUserCountDF: DataFrame): (util.Map[String, String], util.Map[String, String], util.Map[String, String]) = {
    val orgRegisteredUserCountMap = new util.HashMap[String, String]()
    val orgTotalUserCountMap = new util.HashMap[String, String]()
    val orgNameMap = new util.HashMap[String, String]()

    orgUserCountDF.collect().foreach(row => {
      val orgID = row.getAs[String]("orgID")
      orgRegisteredUserCountMap.put(orgID, row.getAs[Long]("registeredCount").toString)
      orgTotalUserCountMap.put(orgID, row.getAs[Long]("totalCount").toString)
      orgNameMap.put(orgID, row.getAs[String]("orgName"))
    })

    (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap)
  }

  /**
   * All courses/programs from elastic search api
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID)
   */
  def allCourseProgramESDataFrame(primaryCategories: Seq[String])(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = elasticSearchCourseProgramDataFrame(primaryCategories)

    // now that error handling is done, proceed with business as usual
    df = df.select(
      col("identifier").alias("courseID"),
      col("primaryCategory").alias("category"),
      col("name").alias("courseName"),
      col("status").alias("courseStatus"),
      col("reviewStatus").alias("courseReviewStatus"),
      col("channel").alias("courseOrgID")
      // col("duration").alias("courseDuration"),
      // col("leafNodesCount").alias("courseResourceCount")
    )
    df = df.dropDuplicates("courseID", "category")
    // df = df.na.fill(0.0, Seq("courseDuration")).na.fill(0, Seq("courseResourceCount"))

    show(df, "allCourseProgramESDataFrame")
    df
  }

  /**
   * Attach org info to course/program data
   * @param allCourseProgramESDF DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID)
   * @param orgDF DataFrame(orgID, orgName, orgStatus)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus)
   */
  def allCourseProgramDataFrame(allCourseProgramESDF: DataFrame, orgDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    val joinOrgDF = orgDF.select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )
    val df = allCourseProgramESDF.join(joinOrgDF, Seq("courseOrgID"), "left")

    show(df, "allCourseProgramDataFrame")
    df
  }

  /* schema definitions for courseDetailsDataFrame */
  val courseHierarchySchema: StructType = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("duration", StringType, nullable = true),
    StructField("leafNodesCount", IntegerType, nullable = true),
    StructField("competencies_v3", StringType, nullable = true)
  ))
  /**
   * course details with competencies json from cassandra dev_hierarchy_store:content_hierarchy
   * @param allCourseProgramDF Dataframe(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   */
  def allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val rawCourseDF = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraContentHierarchyTable)
      .select(col("identifier").alias("courseID"), col("hierarchy"))

    // inner join so that we only retain live courses
    var df = allCourseProgramDF.join(rawCourseDF, Seq("courseID"), "left")

    df = df.na.fill("{}", Seq("hierarchy"))
    df = df.withColumn("data", from_json(col("hierarchy"), courseHierarchySchema))
    df = df.select(
      col("courseID"), col("category"), col("courseName"), col("courseStatus"),
      col("courseReviewStatus"), col("courseOrgID"), col("courseOrgName"), col("courseOrgStatus"),

      col("data.duration").cast(FloatType).alias("courseDuration"),
      col("data.leafNodesCount").alias("courseResourceCount"),
      col("data.competencies_v3").alias("competenciesJson")
    )
    df = df.na.fill(0.0, Seq("courseDuration")).na.fill(0, Seq("courseResourceCount"))

    show(df, "allCourseProgramDetailsWithCompetenciesJsonDataFrame() = (courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)")
    df
  }

  // only live course ids
  val liveCourseSchema: StructType = StructType(Seq(
    StructField("id",  StringType, nullable = true)
  ))
  def liveCourseDataFrame(allCourseProgramDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = allCourseProgramDF.where(expr("category='Course' and courseStatus='Live'")).select(col("courseID").alias("id")).distinct()

    show(df)
    df
  }

  /**
   * course details without competencies json
   * @param allCourseProgramDetailsWithCompDF course details with competencies json
   *                                          DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                          courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   */
  def allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF: DataFrame): DataFrame = {
    val df = allCourseProgramDetailsWithCompDF.drop("competenciesJson")

    show(df)
    df
  }



  /* schema definitions for courseCompetencyDataFrame */
  val courseCompetenciesSchema: ArrayType = ArrayType(StructType(Seq(
    StructField("id",  StringType, nullable = true),
    StructField("name",  StringType, nullable = true),
    // StructField("description",  StringType, nullable = true),
    // StructField("source",  StringType, nullable = true),
    StructField("competencyType",  StringType, nullable = true),
    // StructField("competencyArea",  StringType, nullable = true),
    // StructField("selectedLevelId",  StringType, nullable = true),
    // StructField("selectedLevelName",  StringType, nullable = true),
    // StructField("selectedLevelSource",  StringType, nullable = true),
    StructField("selectedLevelLevel",  StringType, nullable = true)
    //StructField("selectedLevelDescription",  StringType, nullable = true)
  )))
  /**
   * course competency mapping data from cassandra dev_hierarchy_store:content_hierarchy
   * @param allCourseProgramDetailsWithCompDF course details with competencies json
   *                                          DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                          courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competenciesJson)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)
   */
  def allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = allCourseProgramDetailsWithCompDF.filter(col("competenciesJson").isNotNull)
    df = df.withColumn("competencies", from_json(col("competenciesJson"), courseCompetenciesSchema))

    df = df.select(
      col("courseID"), col("category"), col("courseName"), col("courseStatus"),
      col("courseReviewStatus"), col("courseOrgID"), col("courseOrgName"), col("courseOrgStatus"),
      col("courseDuration"), col("courseResourceCount"),
      explode_outer(col("competencies")).alias("competency")
    )
    df = df.filter(col("competency").isNotNull)
    df = df.withColumn("competencyLevel", expr("TRIM(competency.selectedLevelLevel)"))
    df = df.withColumn("competencyLevel",
      expr("IF(competencyLevel RLIKE '[0-9]+', CAST(REGEXP_EXTRACT(competencyLevel, '[0-9]+', 0) AS INTEGER), 1)"))
    df = df.select(
      col("courseID"), col("category"), col("courseName"), col("courseStatus"),
      col("courseReviewStatus"), col("courseOrgID"), col("courseOrgName"), col("courseOrgStatus"),
      col("courseDuration"), col("courseResourceCount"),
      col("competency.id").alias("competencyID"),
      col("competency.name").alias("competencyName"),
      col("competency.competencyType").alias("competencyType"),
      col("competencyLevel")
    )

    show(df, "allCourseProgramCompetencyDataFrame (courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)")
    df
  }

  /**
   *
   * @param allCourseProgramCompetencyDF DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *                                     courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyName, competencyType, competencyLevel)
   * @return DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         competencyID, competencyLevel)
   */
  def liveCourseCompetencyDataFrame(allCourseProgramCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = allCourseProgramCompetencyDF.where(expr("courseStatus='Live' AND category='Course'"))
      .select("courseID", "courseName", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration",
        "courseResourceCount", "competencyID", "competencyLevel")

    show(df, "liveCourseCompetencyDataFrame (courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyLevel)")
    df
  }


  /**
   * data frame of course rating summary
   * @return DataFrame(courseID, categoryLower, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
   */
  def courseRatingSummaryDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingSummaryTable)
      .where(expr("total_number_of_ratings > 0"))
      .withColumn("ratingAverage", expr("sum_of_total_ratings / total_number_of_ratings"))
      .select(
        col("activityid").alias("courseID"),
        col("activitytype").alias("categoryLower"),
        col("sum_of_total_ratings").alias("ratingSum"),
        col("total_number_of_ratings").alias("ratingCount"),
        col("ratingAverage"),
        col("totalcount1stars").alias("count1Star"),
        col("totalcount2stars").alias("count2Star"),
        col("totalcount3stars").alias("count3Star"),
        col("totalcount4stars").alias("count4Star"),
        col("totalcount5stars").alias("count5Star")
      )
    show(df, "courseRatingSummaryDataFrame before duplicate drop")

    df = df.withColumn("categoryLower", lower(col("categoryLower")))
      .dropDuplicates("courseID", "categoryLower")

    show(df, "courseRatingSummaryDataFrame")
    df
  }

  /**
   * add course rating columns to course detail data-frame
   * @param allCourseProgramDetailsDF course details data frame -
   *                                  DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *                                  courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   * @param courseRatingDF course rating summary data frame -
   *                       DataFrame(courseID, ratingSum, ratingCount, ratingAverage,
   *                       count1Star, count2Star, count3Star, count4Star, count5Star)
   * @return DataFrame(courseID, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName,
   *         courseOrgStatus, courseDuration, courseResourceCount, ratingSum, ratingCount, ratingAverage, count1Star,
   *         count2Star, count3Star, count4Star, count5Star)
   */
  def allCourseProgramDetailsWithRatingDataFrame(allCourseProgramDetailsDF: DataFrame, courseRatingDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = allCourseProgramDetailsDF.withColumn("categoryLower", expr("LOWER(category)"))
      .join(courseRatingDF, Seq("courseID", "categoryLower"), "left")

    show(df)
    df
  }


  /**
   *
   * @return DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus)
   */
  def userCourseProgramCompletionDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserEnrolmentsTable)
      .where(expr("active=true"))
      .withColumn("courseCompletedTimestamp", col("completedon").cast("long"))
      .withColumn("courseEnrolledTimestamp", col("enrolled_date").cast("long"))
      .withColumn("lastContentAccessTimestamp", col("lastcontentaccesstime").cast("long"))
      .select(
        col("userid").alias("userID"),
        col("courseid").alias("courseID"),
        col("batchid").alias("batchID"),
        col("progress").alias("courseProgress"),
        col("status").alias("dbCompletionStatus"),
        col("courseCompletedTimestamp"),
        col("courseEnrolledTimestamp"),
        col("lastContentAccessTimestamp")
      ).na.fill(0, Seq("courseProgress"))

    show(df)
    df
  }

  /**
   * get course completion data with details attached
   * @param userCourseProgramCompletionDF  DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus)
   * @param allCourseProgramDetailsDF course details data frame -
   *                                  DataFrame(courseID, category, courseName, courseStatus,
   *                                  courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
   * @param userOrgDF DataFrame(userID, firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus)
   * @return DataFrame(userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp,
   *         courseProgress, dbCompletionStatus, category, courseName, courseStatus, courseReviewStatus, courseOrgID,
   *         courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, firstName, lastName, maskedEmail, userStatus,
   *         userOrgID, userOrgName, userOrgStatus, completionPercentage, completionStatus)
   */
  def allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF: DataFrame, allCourseProgramDetailsDF: DataFrame, userOrgDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    // userID, courseID, batchID, courseCompletedTimestamp, courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus, category, courseName, courseStatus, courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount
    var df = userCourseProgramCompletionDF.join(allCourseProgramDetailsDF, Seq("courseID"), "left")
    show(df, "userAllCourseProgramCompletionDataFrame s=1")

    df = df.join(userOrgDF, Seq("userID"), "left")
      .select("userID", "courseID", "batchID", "courseCompletedTimestamp", "courseEnrolledTimestamp",
        "lastContentAccessTimestamp", "courseProgress", "dbCompletionStatus", "category", "courseName",
        "courseStatus", "courseReviewStatus", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration",
        "courseResourceCount", "firstName", "lastName", "maskedEmail", "userStatus", "userOrgID", "userOrgName", "userOrgStatus")
    df = df.withColumn("completionPercentage", expr("CASE WHEN courseProgress=0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"))
    df = withCompletionStatusColumn(df)

    show(df, "allCourseProgramCompletionWithDetailsDataFrame")

    df
  }

  /**
   *
   * @param allCourseProgramCompletionWithDetailsDF DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *         courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *         completionStatus)
   * @return DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *         courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *         firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *         completionStatus)
   */
  def liveRetiredCourseCompletionWithDetailsDataFrame(allCourseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val df = allCourseProgramCompletionWithDetailsDF.where(expr("courseStatus in ('Live', 'Retired') AND category='Course'"))
    show(df, "liveRetiredCourseCompletionWithDetailsDataFrame")
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them from druid
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   */
  val expectedCompetencySchema: StructType = StructType(Seq(
    StructField("orgID",  StringType, nullable = true),
    StructField("workOrderID",  StringType, nullable = true),
    StructField("userID",  StringType, nullable = true),
    StructField("competencyID",  StringType, nullable = true),
    StructField("expectedCompetencyLevel",  IntegerType, nullable = true)
  ))
  def expectedCompetencyDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    val query = """SELECT edata_cb_data_deptId AS orgID, edata_cb_data_wa_id AS workOrderID, edata_cb_data_wa_userId AS userID, edata_cb_data_wa_competency_id AS competencyID, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS expectedCompetencyLevel FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (df == null) return emptySchemaDataFrame(expectedCompetencySchema)

    df = df.filter(col("competencyID").isNotNull && col("expectedCompetencyLevel").notEqual(0))
      .withColumn("expectedCompetencyLevel", expr("CAST(expectedCompetencyLevel as INTEGER)"))  // Important to cast as integer otherwise a cast will fail later on
      .filter(col("expectedCompetencyLevel").isNotNull && col("expectedCompetencyLevel").notEqual(0))

    show(df)
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them, including live course count
   * @param expectedCompetencyDF expected competency data frame -
   *                             DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName,
   *                               courseOrgStatus, courseDuration, courseResourceCount, competencyID, competencyLevel)
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel, liveCourseCount)
   */
  def expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF: DataFrame, liveCourseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    // live course count DF
    val liveCourseCountDF = expectedCompetencyDF.join(liveCourseCompetencyDF, Seq("competencyID"), "left")
      .where(expr("expectedCompetencyLevel <= competencyLevel"))
      .groupBy("orgID", "workOrderID", "userID", "competencyID", "expectedCompetencyLevel")
      .agg(countDistinct("courseID").alias("liveCourseCount"))

    val df = expectedCompetencyDF.join(liveCourseCountDF, Seq("orgID", "workOrderID", "userID", "competencyID", "expectedCompetencyLevel"), "left")
      .na.fill(0, Seq("liveCourseCount"))

    show(df)
    df
  }

  /* schema definitions for declaredCompetencyDataFrame */
  val profileCompetencySchema: StructType = StructType(Seq(
    StructField("id",  StringType, nullable = true),
    StructField("name",  StringType, nullable = true),
    StructField("status",  StringType, nullable = true),
    StructField("competencyType",  StringType, nullable = true),
    StructField("competencySelfAttestedLevel",  StringType, nullable = true), // this is sometimes an int other times a string
    StructField("competencySelfAttestedLevelValue",  StringType, nullable = true)
  ))
  val profileDetailsSchema: StructType = StructType(Seq(
    StructField("competencies", ArrayType(profileCompetencySchema), nullable = true)
  ))

  object CompLevelParser extends Serializable {

    val competencyLevelPattern: Regex = ".*[Ll]evel[ ]+?([0-9]+).*".r
    /**
     * match string against level pattern and return level or zero
     * @param s string to parse
     * @return level or zero
     */
    def parseCompetencyLevelString(s: String): Int = {
      s match {
        case competencyLevelPattern(level) => level.toInt
        case _ => 0
      }
    }
    /**
     * get competency level from string value
     * @param levelString level string
     * @return level value as int
     */
    def getCompetencyLevel(levelString: String): Int = {
      intOrZero(levelString) match {
        case 0 => parseCompetencyLevelString(levelString)
        case default => default
      }
    }

    /**
     * spark udf to infer competency level value, returns 1 if no value could be inferred
     * @param csaLevel value of competencySelfAttestedLevel column
     * @param csaLevelValue value of competencySelfAttestedLevelValue column
     * @return level value as int
     */
    def compLevelParser(csaLevel: String, csaLevelValue: String): Int = {
      for (levelString <- Seq(csaLevel, csaLevelValue)) {
        val level = getCompetencyLevel(levelString)
        if (level != 0) return level
      }
      1 // return 1 as default
    }
    val compLevelParserUdf: UserDefinedFunction = udf(compLevelParser _)
  }

  /**
   * User's declared competency data from cassandra sunbird:user
   * @return DataFrame(userID, competencyID, declaredCompetencyLevel)
   */
  def declaredCompetencyDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    val userdata = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)

    // select id and profile details column where profile details are available
    var df = userdata.where(col("profiledetails").isNotNull).select("id", "profiledetails")
    // json parse profile details
    df = df.withColumn("profile", from_json(col("profiledetails"), profileDetailsSchema))
    // explode competencies
    df = df.select(col("id"), explode_outer(col("profile.competencies")).alias("competency"))
    // filter out where competency or competency id not present
    df = df.where(col("competency").isNotNull && col("competency.id").isNotNull)

    // use udf for competency level parsing, as the schema for competency level is broken
    df = df.withColumn("declaredCompetencyLevel",
      CompLevelParser.compLevelParserUdf(col("competency.competencySelfAttestedLevel"), col("competency.competencySelfAttestedLevelValue"))
    ).na.fill(1, Seq("declaredCompetencyLevel"))  // if competency is listed without a level assume level 1

    // select useful columns
    df = df.select(
      col("id").alias("userID"),
      col("competency.id").alias("competencyID"),
      col("declaredCompetencyLevel")
    )

    show(df, "declaredCompetencyDataFrame [userID, competencyID, declaredCompetencyLevel]")
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api
   * @return DataFrame(competencyID, competencyName, competencyStatus)
   */
  val fracCompetencySchema: StructType = StructType(Seq(
    StructField("competencyID",  StringType, nullable = true),
    StructField("competencyName",  StringType, nullable = true),
    StructField("competencyStatus",  StringType, nullable = true)
  ))
  def fracCompetencyDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    var df = fracCompetencyDFOption(conf.fracBackendHost).orNull
    if (df == null) return emptySchemaDataFrame(fracCompetencySchema)

    df = df
      .select(explode_outer(col("data.getAllCompetencies")).alias("competency"))
      .select(
        col("competency.id").alias("competencyID"),
        col("competency.name").alias("competencyName"),
        col("competency.status").alias("competencyStatus")
      )
      .where(expr("LOWER(competencyStatus) = 'verified'"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including live course count
   * @param fracCompetencyDF frac competency data frame -
   *                         DataFrame(competencyID, competencyName, competencyStatus)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration,
   *                               courseResourceCount, competencyID, competencyLevel)
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
   */
  def fracCompetencyWithCourseCountDataFrame(fracCompetencyDF: DataFrame, liveCourseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    // live course count DF
    val liveCourseCountDF = fracCompetencyDF.join(liveCourseCompetencyDF, Seq("competencyID"), "left")
      .filter(col("courseID").isNotNull)
      .groupBy("competencyID", "competencyName", "competencyStatus")
      .agg(countDistinct("courseID").alias("liveCourseCount"))

    val df = fracCompetencyDF.join(liveCourseCountDF, Seq("competencyID", "competencyName", "competencyStatus"), "left")
      .na.fill(0, Seq("liveCourseCount"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including officer count
   * @param fracCompetencyWithCourseCountDF frac competency data frame with live course count
   * @param expectedCompetencyDF expected competency data frame
   * @param declaredCompetencyDF declared  competency data frame
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount, officerCountExpected, officerCountDeclared)
   */
  def fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF: DataFrame, expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig) : DataFrame = {
    // fracCompetencyWithCourseCountDF = DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
    // expectedCompetencyDF = DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
    // declaredCompetencyDF = DataFrame(userID, competencyID, declaredCompetencyLevel)

    // add expected officer count
    val fcExpectedCountDF = fracCompetencyWithCourseCountDF.join(expectedCompetencyDF, Seq("competencyID"), "leftouter")
      .groupBy("competencyID", "competencyName", "competencyStatus")
      .agg(countDistinct("userID").alias("officerCountExpected"))

    // add declared officer count
    val fcExpectedDeclaredCountDF = fcExpectedCountDF.join(declaredCompetencyDF, Seq("competencyID"), "leftouter")
      .groupBy("competencyID", "competencyName", "competencyStatus", "officerCountExpected")
      .agg(countDistinct("userID").alias("officerCountDeclared"))

    val df = fracCompetencyWithCourseCountDF.join(fcExpectedDeclaredCountDF, Seq("competencyID", "competencyName", "competencyStatus"), "left")

    show(df)
    df
  }

  /**
   * Calculates user's competency gaps
   * @param expectedCompetencyDF expected competency data frame -
   *                             DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   * @param declaredCompetencyDF declared competency data frame -
   *                             DataFrame(userID, competencyID, declaredCompetencyLevel)
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap)
   */
  def competencyGapDataFrame(expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    var df = expectedCompetencyDF.join(declaredCompetencyDF, Seq("competencyID", "userID"), "left")
    df = df.na.fill(0, Seq("declaredCompetencyLevel"))  // if null values created during join fill with 0
    df = df.groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(
        max("expectedCompetencyLevel").alias("expectedCompetencyLevel"),  // in-case of multiple entries, take max
        max("declaredCompetencyLevel").alias("declaredCompetencyLevel")  // in-case of multiple entries, take max
      )
    df = df.withColumn("competencyGap", expr("expectedCompetencyLevel - declaredCompetencyLevel"))

    show(df)
    df
  }

  /**
   * add course data to competency gap data, add user course completion info on top, calculate user competency gap status
   *
   * @param competencyGapDF competency gap data frame -
   *                        DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap)
   * @param liveCourseCompetencyDF course competency data frame -
   *                               DataFrame(courseID, courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *                               competencyID, competencyLevel)
   * @param allCourseProgramCompletionWithDetailsDF user course completion data frame -
   *                                                DataFrame(userID, courseID, courseProgress, dbCompletionStatus, category, courseName, courseStatus,
   *                                                courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount,
   *                                                firstName, lastName, maskedEmail, userStatus, userOrgID, userOrgName, userOrgStatus, completionPercentage,
   *                                                completionStatus)
   *
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, completionPercentage, completionStatus)
   */
  def competencyGapCompletionDataFrame(competencyGapDF: DataFrame, liveCourseCompetencyDF: DataFrame, allCourseProgramCompletionWithDetailsDF: DataFrame): DataFrame = {

    // userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, courseID,
    // courseName, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount, competencyLevel
    val cgCourseDF = competencyGapDF.filter("competencyGap > 0")
      .join(liveCourseCompetencyDF, Seq("competencyID"), "leftouter")
      .filter("expectedCompetencyLevel >= competencyLevel")

    // drop duplicate columns before join
    val courseCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDF.drop("courseName", "courseOrgID", "courseOrgName", "courseOrgStatus", "courseDuration", "courseResourceCount")

    // userID, competencyID, orgID, workOrderID, completionPercentage
    val gapCourseUserStatus = cgCourseDF.join(courseCompletionWithDetailsDF, Seq("userID", "courseID"), "left")
      .groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(max(col("completionPercentage")).alias("completionPercentage"))
      .withColumn("completionPercentage", expr("IF(ISNULL(completionPercentage), 0.0, completionPercentage)"))

    var df = competencyGapDF.join(gapCourseUserStatus, Seq("userID", "competencyID", "orgID", "workOrderID"), "left")

    df = withCompletionStatusColumn(df)

    show(df)
    df
  }


  /***/
  val submitAssessmentRequestSchema: StructType = StructType(Seq(
    StructField("primaryCategory", StringType, nullable = false),
    StructField("courseId", StringType, nullable = false)
  ))

  val submitAssessmentResponseSchema: StructType = StructType(Seq(
    StructField("pass", BooleanType, nullable = false),
    StructField("result", StringType, nullable = false) // check the datatype
  ))

  def userAssessmentDataFrame()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): DataFrame = {

    var df = cassandraTableAsDataFrame("sunbird", "user_assessment_data")
      .select(
        col("userid").alias("assessUserID"),
        col("assessmentid").alias("assessID"),
        col("submitassessmentresponse"),
        col("submitassessmentrequest")
      )
    df = df.withColumn("requestData", from_json(col("submitassessmentrequest"), submitAssessmentRequestSchema))
    df = df.withColumn("responseData", from_json(col("submitassessmentresponse"), submitAssessmentResponseSchema))

    df = df.select(
      col("assessUserID"),
      col("assessID"),
      col("requestData.primaryCategory").alias("assessType"),
      col("requestData.courseId").alias("assessCourseID"),
      col("responseData.pass").alias("assessPass"),
      col("responseData.result").alias("assessPercentageScore")
    )

    show(df)
    df
  }

}
