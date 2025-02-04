package org.ekstep.analytics.dashboard.odcs

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.{Instant, LocalDate, ZoneOffset, ZonedDateTime, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object OdcsRecomendationModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.odcs.OdcsRecomendationModel"

  override def name() = "OdcsRecomendationModel"
  def processData(timestamp: Long) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    try{
    val df = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraFrameworkHierarchyTable)
    val today = getDate()
    def readCSV(path: String): DataFrame = {spark.read.option("header", "true").csv(path)}

    // Define schema for association's additionalProperties (dynamic fields)
    val competencyAreaSchema = StructType(Seq(
      StructField("identifier", StringType, nullable = true),
      StructField("code", StringType, nullable = true),
      StructField("translations", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("index", IntegerType, nullable = true),
      StructField("refId", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("status", StringType, nullable = true)
    ))

    val additionalPropertiesSchema = StructType(Seq(
      StructField("timeStamp", StringType, nullable = true),
      StructField("previousCategoryCode", StringType, nullable = true),
      StructField("previousTermCode", StringType, nullable = true),
      StructField("importedOn", StringType, nullable = true),
      StructField("importedByName", StringType, nullable = true),
      StructField("importedById", StringType, nullable = true),
      StructField("competencyArea", competencyAreaSchema, nullable = true)
    ))

    val associationSchema = StructType(Seq(
      StructField("identifier", StringType, nullable = true),
      StructField("code", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("refType", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("index", IntegerType, nullable = true),
      StructField("additionalProperties", additionalPropertiesSchema, nullable = true),
      StructField("refId", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("status", StringType, nullable = true)
    ))

    val termSchema = StructType(Seq(
      StructField("identifier", StringType, nullable = true),
      StructField("code", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("refType", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("index", IntegerType, nullable = true),
      StructField("additionalProperties", MapType(StringType, StringType), nullable = true),
      StructField("refId", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("associations", ArrayType(associationSchema), nullable = true)
    ))

    val categorySchema = StructType(Seq(
      StructField("identifier", StringType, nullable = true),
      StructField("code", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("refType", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("index", IntegerType, nullable = true),
      StructField("additionalProperties", MapType(StringType, StringType), nullable = true),
      StructField("refId", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("terms", ArrayType(termSchema), nullable = true)
    ))

    // Final schema for the hierarchy
    val hierarchySchema = StructType(Seq(
      StructField("identifier", StringType, nullable = true),
      StructField("code", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("categories", ArrayType(categorySchema), nullable = true),
      StructField("translations", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("objectType", StringType, nullable = true)
    ))

    // Read the data with the defined schema
    val parsedDF = df.withColumn("hierarchy", from_json(col("hierarchy"), hierarchySchema))


    val orgDesignationDF = parsedDF
      .withColumn("categories", explode(col("hierarchy.categories"))) // Exploding categories
      .withColumn("associations", explode(col("categories.terms.associations"))) // Exploding terms.associations
      .filter(col("categories.code") === "org") // Filter for "org" code in categories
      .select(
        col("hierarchy.identifier").alias("org"), // org comes from hierarchy.identifier
        explode(col("associations.name")).alias("designation")  // designation comes from associations.name
      )

    val competencyDF = parsedDF
      .withColumn("categories", explode(col("hierarchy.categories"))) // Exploding categories
      .filter(col("categories.code") === "designation") // Filter for "designation" code in categories
      .withColumn("terms", explode(col("categories.terms"))) // Exploding terms
      .join( orgDesignationDF, col("hierarchy.identifier").startsWith(col("org")) && col("terms.name") === col("designation"), "inner")
      .withColumn("associations", explode(col("terms.associations"))) // Exploding terms.associations
      .select(
        col("org"), // org from orgDesignationDF
        col("designation"), // designation from orgDesignationDF
        col("associations.name").alias("competency"), // competency comes from associations.name
        col("associations.additionalProperties.competencyArea.name").alias("competencyArea") // competencyArea comes from categories.name
      )

    // Step 4: Extract subtheme from the third category where code = "competency"
    val subthemeDF = parsedDF
      .withColumn("categories", explode(col("hierarchy.categories"))) // Exploding categories
      .filter(col("categories.code") === "competency") // Filter for "competency" code in categories
      .withColumn("terms", explode(col("categories.terms"))) // Exploding terms array
      .join(competencyDF, col("terms.identifier").startsWith(col("org")) && col("terms.name") === col("competency"), "inner")
      .withColumn("associations", explode(col("terms.associations"))) // Exploding terms.associations to get subtheme
      .select(
        col("org"), // org from orgDesignationDF
        col("designation"), // designation from orgDesignationDF
        col("competency"), // competency from orgDesignationDF
        col("competencyArea"), // competencyArea from orgDesignationDF
        col("associations.name").alias("subtheme") // subtheme comes from associations.name
      )


    val kcmContentCompetencyMappingDF = warehouseCache.load(conf.dwKcmContentTable).select(col("course_id"), col("competency_area_id"), col("competency_theme_id"), col("competency_sub_theme_id"))
    val kcmHierarchyDF = warehouseCache.load(conf.dwKcmDictionaryTable)

    val contentCompetencyMappingIdWithNames = kcmContentCompetencyMappingDF
      .join(kcmHierarchyDF, Seq("competency_theme_id", "competency_sub_theme_id"), "inner")
      .select(
        col("course_id"),
        col("competency_theme_id"),
        col("competency_theme"),
        col("competency_sub_theme_id"),
        col("competency_sub_theme"))

    val contentDetailsDF = warehouseCache.load(conf.dwCourseTable)
    val contentDetailsWithOrgDF = contentCompetencyMappingIdWithNames.withColumnRenamed("course_id", "content_id")
      .join(contentDetailsDF, Seq("content_id"), "inner").filter(col("content_sub_type") === "Course")
      .select(
        col("content_id"),
        col("content_provider_id"),
        col("competency_theme_id"),
        col("competency_theme"),
        col("competency_sub_theme_id"),
        col("competency_sub_theme"))

    val joinedDF = subthemeDF.join(contentDetailsWithOrgDF,
      (subthemeDF("competency") === contentDetailsWithOrgDF("competency_theme")) &&
        (subthemeDF("subtheme") === contentDetailsWithOrgDF("competency_sub_theme"))).select(
      col("org"),
      col("designation"),
      col("competency"),
      col("competencyArea"),
      col("subtheme"),
      col("content_id"),
      col("content_provider_id"))



    val currentDateTime = LocalDateTime.now()
    val startOfDay = currentDateTime.minusDays(90).toLocalDate().atStartOfDay()
    // Format the date-time to the desired format (yyyy-MM-dd HH:mm:ss)
    val formattedDateTime = startOfDay.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    println(formattedDateTime)


    val timeUUIDToFormattedDate = udf((timeUUID: String) => {
      val uuidTimestampMillis = UUID.fromString(timeUUID).timestamp()
      val epochMillis = (uuidTimestampMillis - 0x01b21dd213814000L) / 10000
      val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC)
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      dateTime.format(formatter)})

    val ratingDraftDF = cache.load("rating").withColumn("formattedDate", timeUUIDToFormattedDate(col("updatedon")))

    val ratingDF = ratingDraftDF.filter(col("activitytype") === "Course" && col("formattedDate") >= formattedDateTime)
      .groupBy("activityid")
      .agg(
        count("*").alias("rating_count"), sum("rating").alias("total_rating"))
      .withColumn("avg_rating", col("total_rating") / col("rating_count"))  // average rating
      .select(
        col("activityid").alias("content_id"),
        col("rating_count"),
        col("avg_rating")  // adding the avg_rating column
      )

    val enrollmentDetails = warehouseCache.load(conf.dwEnrollmentsTable)
      .withColumn("content_progress_percentage", col("content_progress_percentage").cast("float"))
      .withColumn("user_rating", col("user_rating").cast("float"))
      .withColumn("resource_count_consumed", col("resource_count_consumed").cast("int"))
      .withColumn("live_cbp_plan_mandate", col("live_cbp_plan_mandate").cast("boolean"))
      .filter(col("content_id").isNotNull && col("enrolled_on") >= formattedDateTime)


    val enrollmentDetailsWithCountsDF = enrollmentDetails
      .groupBy("content_id")
      .agg(
        count("*").alias("enrolment_count"),
        count(when(col("user_consumption_status") === "completed", 1)).alias("completed_count"))
      .withColumn("completion_percentage", (col("completed_count") / col("enrolment_count") * 100).cast("double"))

    val cbPlan = warehouseCache.load(conf.dwCBPlanTable)
    val finalDFWithOrgPartUnfiltered = joinedDF.withColumn("org_part", split(col("org"), "_").getItem(0))

    val finalDFWithOrgPart = finalDFWithOrgPartUnfiltered.filter(
      (col("org_part") === col("content_provider_id")) ||
        (col("org_part") =!= col("content_provider_id") && col("competencyArea").isin("Behavioural", "Functional"))
    )

    // Step 2: Filter and group by org, designation, competency, and subtheme, and get distinct content_ids

    val distinctContentDF = finalDFWithOrgPart
      .groupBy("org", "designation", "competency", "subtheme")
      .agg(
        collect_set(col("content_id")).alias("distinct_content_ids") // Collect content IDs into a set and alias it
      )
      .withColumn(
        "distinct_content_ids",
        when(size(col("distinct_content_ids")) === 0, array()).otherwise(col("distinct_content_ids"))
      )
      .withColumn(
        "distinct_content_count",
        size(col("distinct_content_ids")) // Get the count directly from the list size
      )

    val cbpPlanJoinedDF = distinctContentDF
      .join(cbPlan, col("designation") === col("allotment_to") && col("status") === "Live", "left") // Join for all rows
      .withColumn("updated_content_ids",
        when(col("distinct_content_count") < 20,
          array_union(col("distinct_content_ids"), array(col("content_id")))
        ).otherwise(col("distinct_content_ids"))
      )
      .select(
        distinctContentDF("org"),
        distinctContentDF("designation"),
        distinctContentDF("competency"),
        distinctContentDF("subtheme"),
        col("updated_content_ids").alias("distinct_content_ids"),
        col("distinct_content_count")
      )


    //list of groups an designation (find the designations in the same group and fetch the content)
    val otherDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraGroupDesignationTable)
    // Convert content_ids_array (string) to an array of strings
    val otherDFWithArray = otherDF.withColumn("content_ids_array", split(col("content_ids_array"), ","))

    val extendedDF = cbpPlanJoinedDF
      .join(
        otherDFWithArray.filter(col("content_ids_array").isNotNull && size(col("content_ids_array")) > 0), // Only include rows where content_ids_array is non-null and non-empty
        Seq("designation"), "left"
      )
      .withColumn(
        "final_content_ids",
        when(
          col("distinct_content_count") < 20 && col("content_ids_array").isNotNull,
          array_distinct(array_union(col("distinct_content_ids"), col("content_ids_array")))
        ).otherwise(col("distinct_content_ids"))
      )
      .select(
        cbpPlanJoinedDF("org"),
        cbpPlanJoinedDF("designation"),
        cbpPlanJoinedDF("competency"),
        cbpPlanJoinedDF("subtheme"),
        col("final_content_ids").alias("distinct_content_ids"),
        col("distinct_content_count")
      )
    val explodedDF = extendedDF.withColumn("content_id", explode(col("distinct_content_ids"))).filter(col("content_id").isNotNull)

    // Step 2: Join with enrollmentDetailsWithCountsDF on content_id to get the enrolment count, rating count, etc.
    val joinedWithEnrollmentDetailsDF = explodedDF.join(enrollmentDetailsWithCountsDF, Seq("content_id"), "left")
    val joinedDFWithRatingDF = joinedWithEnrollmentDetailsDF.join(ratingDF.withColumnRenamed("activityid", "content_id"), Seq("content_id"), "left")
    val filledDF = joinedDFWithRatingDF.na.fill(0)
    val deduplicatedDF = filledDF.dropDuplicates("org", "designation", "content_id")
    // Step 3: Sort by enrolment_count, rating_count, avg_rating and completion_percentage
    val sortedDF = deduplicatedDF.orderBy(
      col("completion_percentage").desc, // Sort by completion_percentage (descending)
      col("rating_count").desc,    // Sort by rating_count (descending)
      col("avg_rating").desc,     // Sort by avg_rating (descending)
      col("enrolment_count").desc // Sort by enrolment_count (descending)
    )

    val finalDF = sortedDF.groupBy("org", "designation").agg(collect_list("content_id").alias("ordered_content_ids")).withColumn("ordered_content_ids",
      slice(array_distinct(col("ordered_content_ids")), 1, 20)).withColumn("ordered_content_ids",
      concat_ws(",", col("ordered_content_ids")))

    val odcsCourseRecomendationDF = finalDF.select(col("ordered_content_ids").alias("content_ids"), concat(split(col("org"), "_").getItem(0), lit("_"), upper(col("designation"))).alias("org_designation"))
    Redis.dispatchDataFrame[Long]("odcs_course_recomendation", odcsCourseRecomendationDF, "org_designation", "content_ids")
  }catch {
    case e: Exception =>
      // Log the error
      println(s"Error occurred during DataExhaustModel processing: ${e.getMessage}", e)

      // Exit with status 1
      System.exit(1)
  }
 }
}