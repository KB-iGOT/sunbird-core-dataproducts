package org.ekstep.analytics.dashboard.report.assess_new

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil
import org.ekstep.analytics.framework.FrameworkContext

object AllAssessmentTest extends Serializable {

  def main(args: Array[String]): Unit = {
    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext("AllAssessmentTest", config)
    val res = DashboardUtil.Test.time(test(config));
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    AllAssessmentModel.processUserAssessmentData(System.currentTimeMillis(), config)
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "192.168.3.249:9092",
      "compression" -> "none",
      "topics" -> Map(
        "roleUserCount" -> "dev.dashboards.role.count",
        "orgRoleUserCount" -> "dev.dashboards.org.role.count",
        "allCourses" -> "dev.dashboards.course",
        "userCourseProgramProgress" -> "dev.dashboards.user.course.program.progress",
        "fracCompetency" -> "dev.dashboards.competency.frac",
        "courseCompetency" -> "dev.dashboards.competency.course",
        "expectedCompetency" -> "dev.dashboards.competency.expected",
        "declaredCompetency" -> "dev.dashboards.competency.declared",
        "competencyGap" -> "dev.dashboards.competency.gap",
        "userOrg" -> "dev.dashboards.user.org",
        "userAssessment" -> "dev.dashboards.user.assessment",
        "assessment" -> "dev.dashboards.assessment"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "validation" -> "true",

      "redisHost" -> "192.168.3.249",
      "redisPort" -> "6379",
      "redisDB" -> "12",

      "sparkCassandraConnectionHost" -> "192.168.3.211",
      "sparkDruidRouterHost" -> "192.168.3.91",
      "sparkElasticsearchConnectionHost" -> "192.168.3.211",
      "fracBackendHost" -> "frac-dictionary.karmayogi.nic.in",

      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "dev_hierarchy_store",

      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",
      "cassandraUserAssessmentTable" -> "user_assessment_data",

      "store" -> "s3",
      "container" -> "igot",
      "key" -> "aws_storage_key",
      "secret" -> "aws_storage_secret",

      "userReportPath" -> "standalone-reports/user-report",
      "userEnrolmentReportPath" -> "standalone-reports/user-enrollment-report",
      "courseReportPath" -> "standalone-reports/course-report",
      "cbaReportPath" -> "standalone-reports/cba-report",
      "taggedUsersPath" -> "tagged-users/",
      "standaloneAssessmentReportPath" -> "standalone-reports/user-assessment-report-cbp",

      "mdoIDs" -> "0135071359030722569,01358993635114188855",

      "sideOutput" -> sideOutput
    )
    modelParams
  }

}
