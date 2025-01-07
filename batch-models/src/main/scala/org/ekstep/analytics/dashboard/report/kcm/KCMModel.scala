package org.ekstep.analytics.dashboard.report.kcm

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext

object KCMModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.kcm.KCMModel"
  override def name() = "KCMModel"
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val today = getDate()
    val reportPathContentCompetencyMapping = s"${conf.kcmReportPath}/${today}/ContentCompetencyMapping"
    val reportPathCompetencyHierarchy = s"${conf.kcmReportPath}/${today}/CompetencyHierarchy"
    val fileName = "ContentCompetencyMapping"

    // Content - Competency Mapping data
    val categories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program")
    val cbpDetails = allCourseProgramESDataFrame(categories)
      .where("courseStatus IN ('Live', 'Retired')")
      .select("courseID", "competencyAreaRefId", "competencyThemeRefId", "competencySubThemeRefId", "courseName")
    // explode area, theme and sub theme seperately
    val areaExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyAreaRefId) as (pos, competency_area_id)")).repartition(col("courseID"))
    val themeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyThemeRefId) as (pos, competency_theme_id)")).repartition(col("courseID"))
    val subThemeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencySubThemeRefId) as (pos, competency_sub_theme_id)")).repartition(col("courseID"))
    // Joining area, theme and subtheme based on position
    val competencyJoinedDF = areaExploded.join(themeExploded, Seq("courseID", "pos")).join(subThemeExploded, Seq("courseID", "pos"))
    // joining with cbpDetails for getting courses with no competencies mapped to it
    val competencyContentMappingDF = cbpDetails
      .join(competencyJoinedDF, Seq("courseID"), "left")
      .dropDuplicates(Seq("courseID", "competency_area_id", "competency_theme_id", "competency_sub_theme_id")).cache()
    val contentMappingDF = competencyContentMappingDF.withColumn("data_last_generated_on", currentDateTime)
      .select(col("courseID").alias("course_id"), col("competency_area_id"), col("competency_theme_id"), col("competency_sub_theme_id"), col("data_last_generated_on"))

    // changes for creating avro file for warehouse
    warehouseCache.write(contentMappingDF.coalesce(1), conf.dwKcmContentTable)

    val kcmV6 = cache.load("kcmV6").withColumn("hierarchy", from_json(col("hierarchy"), Schema.kcmSchema))
    val kcmArea = kcmV6.withColumn("competencyAreaData", col("hierarchy.categories")(0))
      .withColumn("termsExploded", explode(col("competencyAreaData.terms")))
      .withColumn("associatedTheme", explode(col("termsExploded.associations")))
      .select(col("termsExploded.refId").alias("areaID"),
        col("termsExploded.name").alias("areaName"),
        col("termsExploded.description").alias("areaDescription"),
        col("associatedTheme.refId").alias("themeID"),
        col("associatedTheme.name").alias("themeName")
      )

    val kcmTheme = kcmV6.withColumn("competencyThemeData", col("hierarchy.categories")(1))
      .withColumn("termsExploded", explode(col("competencyThemeData.terms")))
      .withColumn("associatedSubTheme", explode(col("termsExploded.associations")))
      .select(col("termsExploded.refId").alias("themeID"),
        col("termsExploded.name").alias("themeName"),
        col("termsExploded.description").alias("themeDescription"),
        col("associatedSubTheme.refId").alias("subThemeID"),
        col("associatedSubTheme.name").alias("subThemeName"),
        col("associatedSubTheme.description").alias("subThemeDescription")
      )
    val competencyDetailsDF = kcmArea.join(kcmTheme, Seq("themeID", "themeName"), "outer")
      .select(col("areaID").alias("competency_area_id"),col("areaName").alias("competency_area"),col("areaDescription").alias("competency_area_description"),
        col("themeID").alias("competency_theme_id"),col("themeName").alias("competency_theme"),col("themeDescription").alias("competency_theme_description"),
        col("subThemeID").alias("competency_sub_theme_id"),col("subThemeName").alias("competency_sub_theme"),col("subThemeDescription").alias("competency_sub_theme_description")
      ).withColumn("data_last_generated_on", currentDateTime)

    warehouseCache.write(competencyDetailsDF.coalesce(1), conf.dwKcmDictionaryTable)

    // Competency reporting
    val competencyReporting = competencyContentMappingDF.join(competencyDetailsDF, Seq("competency_area_id", "competency_theme_id", "competency_sub_theme_id"))
      .select(
        col("courseID").alias("content_id"),
        col("courseName").alias("content_name"),
        col("competency_area"),
        col("competency_area_description"),
        col("competency_theme"),
        col("competency_theme_description"),
        col("competency_sub_theme"),
        col("competency_sub_theme_description")
      ).orderBy("content_id")

    generateReport(competencyReporting, reportPathContentCompetencyMapping, fileName=fileName)

    // Making report sync configurable
//    if (conf.reportSyncEnable) {
//      syncReports(s"${conf.localReportDir}/${reportPathContentCompetencyMapping}", reportPathContentCompetencyMapping)
//    }
  }
}