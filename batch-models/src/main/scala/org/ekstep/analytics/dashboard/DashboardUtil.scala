package org.ekstep.analytics.dashboard

import org.apache.hadoop.fs.{FileSystem, Path}
import redis.clients.jedis.Jedis
import org.apache.spark.SparkContext
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.storage.CustomS3StorageService
import org.joda.time.{DateTime, DateTimeConstants, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import redis.clients.jedis.exceptions.JedisException
import redis.clients.jedis.params.ScanParams

import java.io.{File, FileWriter, Serializable}
import java.util
import scala.util.Try
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

case class DummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class DummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

case class DashboardConfig (
                             debug: String,
                             validation: String,
                             // kafka connection config
                             broker: String,
                             compression: String,
                             // redis connection config
                             redisHost: String,
                             redisPort: Int,
                             redisDB: Int,
                             // for blob storage
                             store: String,
                             container: String,
                             key: String,
                             secret: String,
                             // other hosts connection config
                             sparkCassandraConnectionHost: String, sparkDruidRouterHost: String,
                             sparkElasticsearchConnectionHost: String, sparkElasticsearchAuditConnectionHost: String, fracBackendHost: String,
                             sparkMongoConnectionHost: String, mlSparkDruidRouterHost: String,
                             mlSparkMongoConnectionHost: String,
                             // kafka topics
                             roleUserCountTopic: String, orgRoleUserCountTopic: String,
                             allCourseTopic: String, userCourseProgramProgressTopic: String,
                             fracCompetencyTopic: String, courseCompetencyTopic: String, expectedCompetencyTopic: String,
                             declaredCompetencyTopic: String, competencyGapTopic: String, userOrgTopic: String, orgTopic: String,
                             userAssessmentTopic: String, assessmentTopic: String, acbpEnrolmentTopic: String,
                             // cassandra key spaces
                             cassandraUserKeyspace: String,
                             cassandraCourseKeyspace: String, cassandraHierarchyStoreKeyspace: String,
                             cassandraUserFeedKeyspace: String,
                             // cassandra table details
                             cassandraUserTable: String, cassandraUserRolesTable: String, cassandraOrgTable: String,
                             cassandraUserEnrolmentsTable: String, cassandraContentHierarchyTable: String,
                             cassandraRatingSummaryTable: String, cassandraUserAssessmentTable: String,
                             cassandraRatingsTable: String, cassandraOrgHierarchyTable: String,
                             cassandraUserFeedTable: String, cassandraAcbpTable: String,
                             cassandraCourseBatchTable: String,
                             cassandraLearnerStatsTable: String,
                             cassandraKarmaPointsTable: String,
                             cassandraHallOfFameTable: String,
                             cassandraKarmaPointsLookupTable: String,
                             cassandraKarmaPointsSummaryTable: String,
                             cassandraLearnerLeaderBoardTable: String,
                             cassandraLearnerLeaderBoardLookupTable: String,
                             cassandraMDOLearnerLeaderboardTable: String,
                             cassandraPublicUserAssessmentDataTable: String,
                             cassandraUserEntityEnrolmentTable: String,
                             cassandraNLWUserLeaderboardTable: String,
                             cassandraNLWMdoLeaderboardTable: String,
                             cassandraOldAssesmentTable: String,
                             cassandraFrameworkHierarchyTable: String,
                             cassandraGroupDesignationTable: String,
                             cassandraSLWMdoTopLearnerTable: String,
                             cassandraSLWMdoLeaderboardTable: String,

                             //warehouse tables;
                             appPostgresHost: String,
                             appPostgresSchema: String,
                             appPostgresUsername: String,
                             appPostgresCredential: String,
                             appOrgHierarchyTable: String,
                             dwPostgresHost: String,
                             dwPostgresSchema: String,
                             dwPostgresUsername: String,
                             dwPostgresCredential: String,
                             dwUserActivityTable: String,
                             dwUserTable: String,
                             dwCourseTable: String,
                             dwEnrollmentsTable: String,
                             dwOrgTable: String,
                             dwAssessmentTable: String,
                             dwBPEnrollmentsTable: String,
                             dwKcmDictionaryTable: String,
                             dwKcmContentTable: String,
                             dwContentResourceTable: String,
                             dwCBPlanTable: String,
                             postgresCompetencyTable: String,
                             postgresCompetencyHierarchyTable: String,
                             dwEventsTable: String,
                             dwEventsEnrolmentTable: String,
                             dwLearnerStatsTable: String,
                             dwSLWMdoLeaderboardTable: String,
                             dwSLWMdoTopLearnerTable: String,
                             dwNLWUserLeaderboardTable: String,

                             // redis keys
                             redisRegisteredOfficerCountKey: String, redisTotalOfficerCountKey: String, redisOrgNameKey: String,
                             redisTotalRegisteredOfficerCountKey: String, redisTotalOrgCountKey: String,
                             redisExpectedUserCompetencyCount: String, redisDeclaredUserCompetencyCount: String,
                             redisUserCompetencyDeclarationRate: String, redisOrgCompetencyDeclarationRate: String,
                             redisUserCompetencyGapCount: String, redisUserCourseEnrolmentCount: String,
                             redisUserCompetencyGapEnrolmentRate: String, redisOrgCompetencyGapEnrolmentRate: String,
                             redisUserCourseCompletionCount: String, redisUserCompetencyGapClosedCount: String,
                             redisUserCompetencyGapClosedRate: String, redisOrgCompetencyGapClosedRate: String,

                             // mongoDB configurations
                             mongoDBCollection: String,
                             mongoDatabase: String,
                             platformRatingSurveyId: String,

                             // for reports
                             mdoIDs: String,
                             localReportDir: String,
                             standaloneAssessmentReportPath: String,
                             userReportPath: String,
                             userEnrolmentReportPath: String,
                             courseReportPath: String,
                             taggedUsersPath: String,
                             cbaReportPath: String,
                             blendedReportPath: String,
                             orgHierarchyReportPath: String,
                             commsConsoleReportPath: String,
                             acbpReportPath: String,
                             acbpMdoEnrolmentReportPath: String,
                             acbpMdoSummaryReportPath: String,
                             kcmReportPath: String,
                             validationReportPath:String,

                             commsConsolePrarambhEmailSuffix: String,
                             commsConsoleNumDaysToConsider: Int,
                             commsConsoleNumTopLearnersToConsider: Int,
                             commsConsolePrarambhTags: String,
                             commsConsolePrarambhNCount: Int,
                             commsConsolePrarambhCbpIds: String,
                             esFormDataIds: String,

                             //ml report config
                             gracePeriod: String,
                             solutionIDs: String,
                             baseUrlForEvidences: String,
                             mlMongoDatabase: String,
                             surveyCollection: String,
                             reportConfigCollection: String,
                             mlReportPath: String,
                             includeExpiredSolutionIDs: Boolean,
                             SurveyQuestionReportBatchSize: String,
                             SurveyStatusReportBatchSize: String,
                             ObservationQuestionReportBatchSize: String,
                             ObservationStatusReportBatchSize: String,
                             nationalLearningWeekStart: String,
                             nationalLearningWeekEnd: String,
                             stateLearningWeekStart: String,
                             stateLearningWeekEnd: String,
                             sizeBucketString: String,
                             // Anonymous Assessment
                             anonymousAssessmentLoggedInUserContentIDs: String,
                             anonymousAssessmentNonLoggedInUserAssessmentIDs: String,


                             prefixDirectoryPath: String,
                             destinationDirectoryPath: String,
                             directoriesToSelect: String,
                             password: String,
                             // for weekly claps
                             cutoffTime: Float,
                             // to enable/disable report sync
                             reportSyncEnable: Boolean,
                             // script path to push data to bq
                             bqScriptPath: String,
                             warehouseReportPath: String,
                             destinationFullReportPath: String,
                             trendingEventsCount: Int,
                             featuredEventsCount: Int
                           ) extends Serializable

object DashboardConfigParser extends Serializable {
  /* Config functions */
  def getConfig[T](config: Map[String, AnyRef], key: String, default: T = null): T = {
    val path = key.split('.')
    var obj = config
    path.slice(0, path.length - 1).foreach(f => { obj = obj.getOrElse(f, Map()).asInstanceOf[Map[String, AnyRef]] })
    obj.getOrElse(path.last, default).asInstanceOf[T]
  }
  def getConfigModelParam(config: Map[String, AnyRef], key: String, default: String = ""): String = getConfig[String](config, key, default)
  def getConfigSideBroker(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.brokerList", "")
  def getConfigSideBrokerCompression(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.compression", "snappy")
  def getConfigSideTopic(config: Map[String, AnyRef], key: String): String = getConfig[String](config, s"sideOutput.topics.${key}", "")

  def parseConfig(config: Map[String, AnyRef]): DashboardConfig = {
    DashboardConfig(
      debug = getConfigModelParam(config, "debug"),
      validation = getConfigModelParam(config, "validation"),
      // kafka connection config
      broker = getConfigSideBroker(config),
      compression = getConfigSideBrokerCompression(config),
      // redis connection config
      redisHost = getConfigModelParam(config, "redisHost"),
      redisPort = getConfigModelParam(config, "redisPort").toInt,
      redisDB = getConfigModelParam(config, "redisDB").toInt,
      //for blob storage
      store = getConfigModelParam(config, "store"),
      container = getConfigModelParam(config, "container"),
      key = getConfigModelParam(config, "key"),
      secret = getConfigModelParam(config, "secret"),
      // other hosts connection config
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      sparkDruidRouterHost = getConfigModelParam(config, "sparkDruidRouterHost"),
      sparkElasticsearchConnectionHost = getConfigModelParam(config, "sparkElasticsearchConnectionHost"),
      sparkElasticsearchAuditConnectionHost = getConfigModelParam(config, "sparkElasticsearchAuditConnectionHost"),
      sparkMongoConnectionHost =  getConfigModelParam(config, "sparkMongoConnectionHost"),
      fracBackendHost = getConfigModelParam(config, "fracBackendHost"),
      mlSparkDruidRouterHost = getConfigModelParam(config, "mlSparkDruidRouterHost"),
      mlSparkMongoConnectionHost = getConfigModelParam(config, "mlSparkMongoConnectionHost"),
      // kafka topics
      roleUserCountTopic = getConfigSideTopic(config, "roleUserCount"),
      orgRoleUserCountTopic = getConfigSideTopic(config, "orgRoleUserCount"),
      allCourseTopic = getConfigSideTopic(config, "allCourses"),
      userCourseProgramProgressTopic = getConfigSideTopic(config, "userCourseProgramProgress"),
      fracCompetencyTopic = getConfigSideTopic(config, "fracCompetency"),
      courseCompetencyTopic = getConfigSideTopic(config, "courseCompetency"),
      expectedCompetencyTopic = getConfigSideTopic(config, "expectedCompetency"),
      declaredCompetencyTopic = getConfigSideTopic(config, "declaredCompetency"),
      competencyGapTopic = getConfigSideTopic(config, "competencyGap"),
      userOrgTopic = getConfigSideTopic(config, "userOrg"),
      orgTopic = getConfigSideTopic(config, "org"),
      userAssessmentTopic = getConfigSideTopic(config, "userAssessment"),
      assessmentTopic = getConfigSideTopic(config, "assessment"),
      acbpEnrolmentTopic = getConfigSideTopic(config, "acbpEnrolment"),

      //Newly added for the datawarehouse job
      appPostgresHost = getConfigModelParam(config, "appPostgresHost"),
      appPostgresSchema = getConfigModelParam(config, "appPostgresSchema"),
      appPostgresUsername = getConfigModelParam(config, "appPostgresUsername"),
      appPostgresCredential = getConfigModelParam(config, "appPostgresCredential"),
      appOrgHierarchyTable = getConfigModelParam(config, "appOrgHierarchyTable"),
      dwPostgresHost = getConfigModelParam(config, "dwPostgresHost"),
      dwPostgresSchema = getConfigModelParam(config, "dwPostgresSchema"),
      dwPostgresUsername = getConfigModelParam(config, "dwPostgresUsername"),
      dwPostgresCredential = getConfigModelParam(config, "dwPostgresCredential"),
      dwUserActivityTable = getConfigModelParam(config, "dwUserActivityTable", "user_activity"),
      dwUserTable = getConfigModelParam(config, "dwUserTable"),
      dwCourseTable = getConfigModelParam(config, "dwCourseTable"),
      dwEnrollmentsTable = getConfigModelParam(config, "dwEnrollmentsTable"),
      dwOrgTable = getConfigModelParam(config, "dwOrgTable"),
      dwAssessmentTable = getConfigModelParam(config, "dwAssessmentTable"),
      dwBPEnrollmentsTable = getConfigModelParam(config, "dwBPEnrollmentsTable"),
      dwKcmDictionaryTable = getConfigModelParam(config, "dwKcmDictionaryTable"),
      dwKcmContentTable = getConfigModelParam(config, "dwKcmContentTable"),
      dwContentResourceTable = getConfigModelParam(config, "dwContentResourceTable"),
      dwCBPlanTable = getConfigModelParam(config, "dwCBPlanTable"),
      postgresCompetencyTable = getConfigModelParam(config, "postgresCompetencyTable"),
      postgresCompetencyHierarchyTable = getConfigModelParam(config, "postgresCompetencyHierarchyTable"),
      dwEventsTable = getConfigModelParam(config, "dwEventsTable"),
      dwEventsEnrolmentTable = getConfigModelParam(config, "dwEventsEnrolmentTable"),
      dwLearnerStatsTable = getConfigModelParam(config, "dwLearnerStatsTable"),
      dwSLWMdoLeaderboardTable = getConfigModelParam(config, "dwSLWMdoLeaderboardTable"),
      dwSLWMdoTopLearnerTable = getConfigModelParam(config, "dwSLWMdoTopLearnerTable"),
      dwNLWUserLeaderboardTable = getConfigModelParam(config, "dwNLWUserLeaderboardTable"),
      // cassandra key spaces
      cassandraUserKeyspace = getConfigModelParam(config, "cassandraUserKeyspace"),
      cassandraCourseKeyspace = getConfigModelParam(config, "cassandraCourseKeyspace"),
      cassandraHierarchyStoreKeyspace = getConfigModelParam(config, "cassandraHierarchyStoreKeyspace"),
      cassandraUserFeedKeyspace = getConfigModelParam(config, "cassandraUserFeedKeyspace"),
      // cassandra table details
      cassandraUserTable = getConfigModelParam(config, "cassandraUserTable"),
      cassandraUserRolesTable = getConfigModelParam(config, "cassandraUserRolesTable"),
      cassandraOrgTable = getConfigModelParam(config, "cassandraOrgTable"),
      cassandraUserEnrolmentsTable = getConfigModelParam(config, "cassandraUserEnrolmentsTable"),
      cassandraContentHierarchyTable = getConfigModelParam(config, "cassandraContentHierarchyTable"),
      cassandraRatingSummaryTable = getConfigModelParam(config, "cassandraRatingSummaryTable"),
      cassandraUserAssessmentTable = getConfigModelParam(config, "cassandraUserAssessmentTable"),
      cassandraRatingsTable = getConfigModelParam(config, "cassandraRatingsTable"),
      cassandraOrgHierarchyTable = getConfigModelParam(config, "cassandraOrgHierarchyTable"),
      cassandraUserFeedTable = getConfigModelParam(config, "cassandraUserFeedTable"),
      cassandraCourseBatchTable = getConfigModelParam(config, "cassandraCourseBatchTable"),
      cassandraLearnerStatsTable = getConfigModelParam(config, "cassandraLearnerStatsTable"),
      cassandraKarmaPointsTable = getConfigModelParam(config, "cassandraKarmaPointsTable"),
      cassandraAcbpTable = getConfigModelParam(config, "cassandraAcbpTable"),
      cassandraHallOfFameTable = getConfigModelParam(config, "cassandraHallOfFameTable"),
      cassandraKarmaPointsLookupTable = getConfigModelParam(config, "cassandraKarmaPointsLookupTable"),
      cassandraKarmaPointsSummaryTable = getConfigModelParam(config, "cassandraKarmaPointsSummaryTable"),
      cassandraLearnerLeaderBoardTable = getConfigModelParam(config, "cassandraLearnerLeaderBoardTable"),
      cassandraLearnerLeaderBoardLookupTable = getConfigModelParam(config, "cassandraLearnerLeaderBoardLookupTable"),
      cassandraMDOLearnerLeaderboardTable = getConfigModelParam(config, "cassandraMDOLearnerLeaderboardTable"),
      cassandraOldAssesmentTable = getConfigModelParam(config, "cassandraOldAssesmentTable"),
      cassandraPublicUserAssessmentDataTable = getConfigModelParam(config, "cassandraPublicUserAssessmentDataTable"),
      cassandraUserEntityEnrolmentTable = getConfigModelParam(config, "cassandraUserEntityEnrolmentTable"),
      cassandraFrameworkHierarchyTable = getConfigModelParam(config, "cassandraFrameworkHierarchyTable"),
      cassandraGroupDesignationTable = getConfigModelParam(config, "cassandraGroupDesignationTable"),
      cassandraSLWMdoTopLearnerTable = getConfigModelParam(config, "cassandraSLWMdoTopLearnerTable"),
      cassandraSLWMdoLeaderboardTable = getConfigModelParam(config, "cassandraSLWMdoLeaderboardTable"),


      // redis keys
      redisRegisteredOfficerCountKey = "mdo_registered_officer_count",
      redisTotalOfficerCountKey = "mdo_total_officer_count",
      redisOrgNameKey = "mdo_name_by_org",
      redisTotalRegisteredOfficerCountKey = "mdo_total_registered_officer_count",
      redisTotalOrgCountKey = "mdo_total_org_count",
      redisExpectedUserCompetencyCount = "dashboard_expected_user_competency_count",
      redisDeclaredUserCompetencyCount = "dashboard_declared_user_competency_count",
      redisUserCompetencyDeclarationRate = "dashboard_user_competency_declaration_rate",
      redisOrgCompetencyDeclarationRate = "dashboard_org_competency_declaration_rate",
      redisUserCompetencyGapCount = "dashboard_user_competency_gap_count",
      redisUserCourseEnrolmentCount = "dashboard_user_course_enrollment_count",
      redisUserCompetencyGapEnrolmentRate = "dashboard_user_competency_gap_enrollment_rate",
      redisOrgCompetencyGapEnrolmentRate = "dashboard_org_competency_gap_enrollment_rate",
      redisUserCourseCompletionCount = "dashboard_user_course_completion_count",
      redisUserCompetencyGapClosedCount = "dashboard_user_competency_gap_closed_count",
      redisUserCompetencyGapClosedRate = "dashboard_user_competency_gap_closed_rate",
      redisOrgCompetencyGapClosedRate = "dashboard_org_competency_gap_closed_rate",

      //mongoBD configurations
      mongoDBCollection =  getConfigModelParam(config, "mongoDBCollection"),
      mongoDatabase = getConfigModelParam(config, "mongoDatabase"),
      platformRatingSurveyId = getConfigModelParam(config, "platformRatingSurveyId"),

      // for reports
      mdoIDs = getConfigModelParam(config, "mdoIDs"),
      localReportDir = getConfigModelParam(config, "localReportDir", "/mount/data/analytics/reports"),
      standaloneAssessmentReportPath = getConfigModelParam(config, "standaloneAssessmentReportPath"),
      userReportPath = getConfigModelParam(config, "userReportPath"),
      userEnrolmentReportPath = getConfigModelParam(config, "userEnrolmentReportPath"),
      courseReportPath = getConfigModelParam(config, "courseReportPath"),
      taggedUsersPath = getConfigModelParam(config, "taggedUsersPath"),
      cbaReportPath = getConfigModelParam(config, "cbaReportPath"),
      blendedReportPath = getConfigModelParam(config, "blendedReportPath"),
      orgHierarchyReportPath = getConfigModelParam(config, "orgHierarchyReportPath"),
      commsConsoleReportPath = getConfigModelParam(config, "commsConsoleReportPath"),
      acbpReportPath = getConfigModelParam(config, "acbpReportPath"),
      acbpMdoEnrolmentReportPath = getConfigModelParam(config, "acbpMdoEnrolmentReportPath"),
      acbpMdoSummaryReportPath = getConfigModelParam(config, "acbpMdoSummaryReportPath"),
      kcmReportPath = getConfigModelParam(config, "kcmReportPath"),
      validationReportPath = getConfigModelParam(config, "validationReportPath"),
      //ml report config
      gracePeriod = getConfigModelParam(config, "gracePeriod"),
      solutionIDs = getConfigModelParam(config, "solutionIDs"),
      baseUrlForEvidences = getConfigModelParam(config, "baseUrlForEvidences"),
      mlMongoDatabase = getConfigModelParam(config, "mlMongoDatabase"),
      surveyCollection = getConfigModelParam(config, "surveyCollection"),
      reportConfigCollection = getConfigModelParam(config, "reportConfigCollection"),
      mlReportPath = getConfigModelParam(config, "mlReportPath"),
      includeExpiredSolutionIDs = getConfigModelParam(config, "includeExpiredSolutionIDs", "true").toBoolean,
      SurveyQuestionReportBatchSize = getConfigModelParam(config, "SurveyQuestionReportBatchSize"),
      SurveyStatusReportBatchSize = getConfigModelParam(config, "SurveyStatusReportBatchSize"),
      ObservationQuestionReportBatchSize = getConfigModelParam(config, "ObservationQuestionReportBatchSize"),
      ObservationStatusReportBatchSize = getConfigModelParam(config, "ObservationStatusReportBatchSize"),

      nationalLearningWeekStart = getConfigModelParam(config, "nationalLearningWeekStart"),
      nationalLearningWeekEnd = getConfigModelParam(config, "nationalLearningWeekEnd"),
      stateLearningWeekStart = getConfigModelParam(config, "stateLearningWeekStart"),
      stateLearningWeekEnd = getConfigModelParam(config, "stateLearningWeekEnd"),
      sizeBucketString = getConfigModelParam(config, "sizeBucketString"),

      cassandraNLWUserLeaderboardTable = getConfigModelParam(config, "cassandraNLWUserLeaderboardTable"),
      cassandraNLWMdoLeaderboardTable = getConfigModelParam(config, "cassandraNLWMdoLeaderboardTable"),



      // comms-console
      commsConsolePrarambhEmailSuffix = getConfigModelParam(config, "commsConsolePrarambhEmailSuffix", ".kb@karmayogi.in"),
      commsConsoleNumDaysToConsider = getConfigModelParam(config, "commsConsoleNumDaysToConsider", "15").toInt,
      commsConsoleNumTopLearnersToConsider = getConfigModelParam(config, "commsConsoleNumTopLearnersToConsider", "60").toInt,
      commsConsolePrarambhTags = getConfigModelParam(config, "commsConsolePrarambhTags", "rojgaar,rozgaar,rozgar"),
      commsConsolePrarambhNCount = getConfigModelParam(config, "commsConsolePrarambhNCount", "6").toInt,
      commsConsolePrarambhCbpIds = getConfigModelParam(config, "commsConsolePrarambhCbpIds", "do_11359618144357580811,do_113569878939262976132,do_113474579909279744117,do_113651330692145152128,do_1134122937914327041177,do_113473120005832704152,do_1136364244148060161889,do_1136364937253437441916"),
      esFormDataIds= getConfigModelParam(config, "esFormDataIds"),
      prefixDirectoryPath = getConfigModelParam(config, "prefixDirectoryPath"),
      destinationDirectoryPath = getConfigModelParam(config, "destinationDirectoryPath"),
      directoriesToSelect = getConfigModelParam(config, "directoriesToSelect"),
      password = getConfigModelParam(config, "password"),

      //Anonymous Assessment
      anonymousAssessmentLoggedInUserContentIDs = getConfigModelParam(config, "anonymousAssessmentLoggedInUserContentIDs"),
      anonymousAssessmentNonLoggedInUserAssessmentIDs = getConfigModelParam(config, "anonymousAssessmentNonLoggedInUserAssessmentIDs"),

      // for weekly claps
      cutoffTime = getConfigModelParam(config, "cutoffTime", "60.0").toFloat,
      // config to enable disable report sync
      reportSyncEnable = getConfigModelParam(config, "reportSyncEnable", "true").toBoolean,
      // Bq script changes
      bqScriptPath = getConfigModelParam(config, "bqScriptPath", "/mount/data/analytics/bq-scripts.sh"),
      // warehouse report path
      warehouseReportPath = getConfigModelParam(config, "warehouseReportPath", "warehouse"),
      destinationFullReportPath = getConfigModelParam(config, "destinationFullReportPath", "/standalone-reports/full_report"),
      trendingEventsCount = getConfigModelParam(config, "trendingEventsCount", "100").toInt,
      featuredEventsCount = getConfigModelParam(config, "featuredEventsCount", "100").toInt
    )
  }
  /* Config functions end */
}

class AvroFSCache(val path: String, val compression: String = "snappy") extends Serializable {
  def write(df: DataFrame, name: String): Unit = {
    df.write.mode(SaveMode.Overwrite).option("compression", compression).format("avro").save(s"${path}/${name}")
  }
  def load(name: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("avro").load(s"${path}/${name}").persist(StorageLevel.MEMORY_ONLY)
    //spark.read.format("avro").load(s"${path}/${name}") // removed the persist to optimise memory usage
  }
}

class ParquetFSCache(val path: String, val compression: String = "snappy") extends Serializable {

  def write(df: DataFrame, name: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("compression", compression)
      .parquet(s"$path/$name")
  }

  def load(name: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .parquet(s"$path/$name")
  }
}

object StorageUtil extends Serializable {

  //  def getStorageService(config: DashboardConfig): BaseStorageService = {
  //    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint_with_protocol")
  //    val storageType = "s3"
  //    val storageKey = AppConf.getConfig(config.key)
  //    val storageSecret = AppConf.getConfig(config.secret)
  //
  //    val storageService = if ("s3".equalsIgnoreCase(storageType) && !"".equalsIgnoreCase(storageEndpoint)) {
  //      new CustomS3StorageService(
  //        StorageConfig(storageType, storageKey, storageSecret, Option(storageEndpoint))
  //      )
  //    } else {
  //      StorageServiceFactory.getStorageService(
  //        StorageConfig(storageType, AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config"))
  //      )
  //    }
  //    storageService
  //  }

  def getStorageService(config: DashboardConfig): BaseStorageService = {
    val storageEndpoint = AppConf.getConfig("cloud_storage_endpoint_with_protocol")
    val storageType = "gcloud"
    val storageKey = AppConf.getConfig(config.key)
    val storageSecret = AppConf.getConfig(config.secret)

    val storageService = if ("s3".equalsIgnoreCase(storageType) && !"".equalsIgnoreCase(storageEndpoint)) {
      new CustomS3StorageService(
        StorageConfig(storageType, storageKey, storageSecret, Option(storageEndpoint))
      )
    } else {
      StorageServiceFactory.getStorageService(
        StorageConfig(storageType, storageKey, storageSecret)
      )
    }
    storageService
  }


  def removeFile(path: String)(implicit spark: SparkSession): Unit = {
    val rmFile = new Path(path)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(rmFile)) {
      fs.delete(rmFile, true)
    }
  }

  def simulateSparkOverwrite(path: String, fileName: String, content: String): Unit = {
    val file = new File(s"${path}/${fileName}")
    // clear parent directory
    if (file.getParentFile.exists()) {
      val directory = new Directory(file.getParentFile)
      directory.deleteRecursively()
    }
    file.getParentFile.mkdirs()
    // create the file
    if (!file.exists()) file.createNewFile()
    // write content
    val fileWriter = new FileWriter(file)
    fileWriter.write(content)
    fileWriter.close()
  }

  def renameCSV(ids: util.List[String], reportTempPath: String, fileName: String, partitionKey: String): Unit = {
    ids.foreach(id => {
      val orgReportPath = new File(s"${reportTempPath}/${partitionKey}=${id}/")
      val csvFiles = if (orgReportPath.exists() && orgReportPath.isDirectory) {
        orgReportPath.listFiles().filter(f => {
          f != null && f.getName != null && f.getName.startsWith("part-") && f.getName.endsWith(".csv")
        }).toList
      } else {
        List[File]()
      }

      csvFiles.zipWithIndex.foreach(csvFileWithIndex => {
        val (csvFile, index) = csvFileWithIndex
        val customizedPath = new File(s"${reportTempPath}/${partitionKey}=${id}/${fileName}${if (index == 0) "" else index}.csv")
        csvFile.renameTo(customizedPath)
      })

    })
  }

  def renameCSVWithoutPartitions(reportTempPath: String, fileName: String): Unit = {
    val orgReportPath = new File(s"${reportTempPath}/")
    val csvFiles = if (orgReportPath.exists() && orgReportPath.isDirectory) {
      orgReportPath.listFiles().filter(f => {
        f != null && f.getName != null && f.getName.startsWith("part-") && f.getName.endsWith(".csv")
      }).toList
    } else {
      List[File]()
    }

    csvFiles.zipWithIndex.foreach(csvFileWithIndex => {
      val (csvFile, index) = csvFileWithIndex
      val customizedPath = new File(s"${reportTempPath}/${fileName}${if (index == 0) "" else index}.csv")
      csvFile.renameTo(customizedPath)
    })
  }

}


object DashboardUtil extends Serializable {

  implicit var debug: Boolean = false
  implicit var validation: Boolean = false

  /**
   * Adds more utility functions to spark DataFrame
   * @param df implicit data frame reference
   */
  implicit class DataFrameMod(df: DataFrame) extends Serializable {

    /**
     * for each value in column `groupByKey`, order rows by `orderByKey` and filter top/bottom rows
     * does not order the final data frame, `rowNumColName` is added
     *
     * @param groupByKey cols to group by
     * @param orderByKey col to order by
     * @param limit number of rows to take from each group
     * @param desc descending flag for the orderByKey
     * @param rowNumColName column name for group wise row numbers
     * @return data frame with the operation applied
     */
    def groupByLimit(groupByKey: Seq[String], orderByKey: String, limit: Int, desc: Boolean = false,
                     rowNumColName: String = "rowNum"): DataFrame = {
      if (groupByKey.isEmpty) throw new Exception("groupByLimit error: groupByKey is empty")

      val ordering = if (desc) {
        df.col(orderByKey).desc
      } else {
        df.col(orderByKey).asc
      }
      df
        .withColumn(rowNumColName, row_number().over(Window.partitionBy(groupByKey.head, groupByKey.tail:_*).orderBy(ordering)))
        .filter(col(rowNumColName) <= limit)
    }

    /**
     * duration format a column
     *
     * @param inCol input column name
     * @param outCol output column name
     * @return data frame with duration formatted column
     */
    def durationFormat(inCol: String, outCol: String = null): DataFrame = {
      val outColName = if (outCol == null) inCol else outCol
      df.withColumn(outColName,
        when(col(inCol).isNull, lit(""))
          .otherwise(
            format_string("%02d:%02d:%02d",
              expr(s"${inCol} / 3600").cast("int"),
              expr(s"${inCol} % 3600 / 60").cast("int"),
              expr(s"${inCol} % 60").cast("int")
            )
          )
      )
    }

    def durationFormatMinutes(inCol: String, outCol: String = null): DataFrame = {
      val outColName = if (outCol == null) inCol else outCol
      df.withColumn(outColName,
        when(col(inCol).isNull, lit(""))
          .otherwise(
            format_string("%02d:%02d:%02d",
              expr(s"${inCol} / 60").cast("int"),
              expr(s"${inCol} % 60").cast("int"),
              lit(0)
            )
          )
      )
    }

    /**
     * collect values in keyField and valueField as a map
     *
     * @param keyField key field
     * @param valueField value field
     * @tparam T type of the value
     * @return java.util.Map[String, String] of keyField and valueField values
     */
    def toMap[T](keyField: String, valueField: String): util.Map[String, String] = {
      df.rdd.map(row => (row.getAs[String](keyField), row.getAs[T](valueField).toString))
        .collectAsMap()
    }

  }

  val cache: AvroFSCache = new AvroFSCache("/mount/data/analytics/cache", "uncompressed")
  val pqCache: ParquetFSCache = new ParquetFSCache("/mount/data/analytics/cache_pq")


  val warehouseCache: AvroFSCache = new AvroFSCache("/mount/data/analytics/warehouse", "uncompressed")
  val warehousePqCache: ParquetFSCache = new ParquetFSCache("/mount/data/analytics/warehouse_pq")


  object Test extends Serializable {
    /**
     * ONLY FOR TESTING!!, do not use to create spark context in model or job
     * */
    def getSessionAndContext(name: String, config: Map[String, AnyRef]): (SparkSession, SparkContext, FrameworkContext) = {
      val cassandraHost = config.getOrElse("sparkCassandraConnectionHost", "localhost").asInstanceOf[String]
      val mongodbHost = config.getOrElse("sparkMongoConnectionHost", "localhost").asInstanceOf[String]
      val esHost = config.getOrElse("sparkElasticsearchConnectionHost", "localhost").asInstanceOf[String]
      val spark: SparkSession =
        SparkSession
          .builder()
          .appName(name)
          .config("spark.master", "local[*]")
          .config("spark.cassandra.connection.host", cassandraHost)
          .config("spark.cassandra.output.batch.size.rows", "10000")
          //.config("spark.cassandra.read.timeoutMS", "60000")
          .config("spark.sql.legacy.json.allowEmptyString.enabled", "true")
          .config("spark.sql.caseSensitive", "true")
          .config("es.nodes", esHost)
          .config("es.port", "9200")
          .config("es.index.auto.create", "false")
          .config("es.nodes.wan.only", "true")
          .config("es.nodes.discovery", "false")
          .config("spark.mongodb.input.uri", s"mongodb://${mongodbHost}/Nodebb.Objects")
          .config("spark.mongodb.input.sampleSize", 50000)
          .getOrCreate()
      val sc: SparkContext = spark.sparkContext
      val fc: FrameworkContext = new FrameworkContext()
      sc.setLogLevel("WARN")
      (spark, sc, fc)
    }

    def time[R](block: => R): (Long, R) = {
      val t0 = System.currentTimeMillis()
      val result = block // call-by-name
      val t1 = System.currentTimeMillis()
      ((t1 - t0), result)
    }

  }

  val allowedContentCategories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment")

  def validateContentCategories(categories: Seq[String]): Unit = {

  }

  /**
   * add a `timestamp` column with give constant value, will override current `timestamp` column if present
   * @param timestamp provided timestamp value
   * @return data frame with timestamp column
   */
  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  def getDate(): String = {
    val dateFormatData: DateTimeFormatter = DateTimeFormat.forPattern(dateFormat).withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormatData.print(System.currentTimeMillis());
  }

  def getThisWeekDates(): (String, String, String, String) = {
    val istTimeZone = DateTimeZone.forID("Asia/Kolkata")
    val currentDate = DateTime.now(istTimeZone)
    val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern(dateFormat).withZone(istTimeZone)
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern(dateTimeFormat).withZone(istTimeZone)
    val dataTillDate = currentDate.minusDays(1)
    val startOfWeek = dataTillDate.withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay()
    val endOfWeek = startOfWeek.plusDays(6).withTime(23, 59, 59, 999)
    (formatter.print(startOfWeek), dateFormatter.print(endOfWeek), formatter.print(endOfWeek), dateFormatter.print(dataTillDate))
  }

  val dateTimeWithAMPMFormat = "yyyy-MM-dd HH:mm:ss a"
  val currentDateTime = date_format(current_timestamp(), dateTimeWithAMPMFormat)
  val dateTimeFormat = "yyyy-MM-dd HH:mm:ss"
  val dateTimeWithMilliSecFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
  val dateFormat = "yyyy-MM-dd"
  val timeFormat = "HH:mm:ss"
  /* Util functions */
  def csvWrite(df: DataFrame, path: String, header: Boolean = true, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    // spark 2.4.x has a bug where the csv does not get written with header row if the data frame is empty, this is a workaround
    if (df.isEmpty) {
      StorageUtil.simulateSparkOverwrite(path, "part-0000-XXX.csv", df.columns.mkString(",") + "\n")
    } else {
      df.write.mode(saveMode).format("csv").option("header", header.toString).save(path)
    }
  }

  def csvWritePartition(df: DataFrame, path: String, partitionKey: String, header: Boolean = true, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    df.write.mode(saveMode).format("csv").option("header", header.toString)
      .partitionBy(partitionKey).save(path)
  }

  def generateReport(df: DataFrame, reportPath: String, partitionKey: String = null, fileName: String = null, fileSaveMode: SaveMode = SaveMode.Overwrite)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    import spark.implicits._
    val reportFullPath = s"${conf.localReportDir}/${reportPath}"
    println(s"REPORT: Writing report to ${reportFullPath} ...")
    if (partitionKey == null) {
      csvWrite(df.coalesce(1), reportFullPath, saveMode = fileSaveMode)
      if (fileName != null) StorageUtil.renameCSVWithoutPartitions(reportFullPath, fileName)
    } else {
      val ids = df.select(partitionKey).distinct().map(_.getString(0)).filter(_.nonEmpty).collectAsList()
      // generate partitioned report
      csvWritePartition(df, reportFullPath, partitionKey)
      if (fileName != null) StorageUtil.renameCSV(ids, reportFullPath, fileName, partitionKey) // rename part-*.csv files to provided name
    }
    StorageUtil.removeFile(s"${reportFullPath}/_SUCCESS") // remove success file
    println(s"REPORT: Finished Writing report to ${reportFullPath}")
  }

  def kafkaDispatch(data: RDD[String], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    if (topic == "") {
      println("ERROR: topic is blank, skipping kafka dispatch")
    } else if (conf.broker == "") {
      println("ERROR: broker list is blank, skipping kafka dispatch")
    } else {
      KafkaDispatcher.dispatch(Map("brokerList" -> conf.broker, "topic" -> topic, "compression" -> conf.compression), data)
    }
  }

  def kafkaDispatch(data: DataFrame, topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    kafkaDispatch(data.toJSON.rdd, topic)
  }

  def kafkaDispatchDS[T](data: Dataset[T], topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    kafkaDispatch(data.toJSON.rdd, topic)
  }

  def apiThrowException(method: String, url: String, body: String): String = {
    val request = method.toLowerCase() match {
      case "post" => new HttpPost(url)
      case _ => throw new Exception(s"HTTP method '${method}' not supported")
    }
    request.setHeader("Content-type", "application/json")  // set the Content-type
    request.setEntity(new StringEntity(body))  // add the JSON as a StringEntity
    val httpClient = HttpClientBuilder.create().build()  // create HttpClient
    val response = httpClient.execute(request)  // send the request
    val statusCode = response.getStatusLine.getStatusCode  // get status code
    if (statusCode < 200 || statusCode > 299) {
      throw new Exception(s"ERROR: got status code=${statusCode}, response=${EntityUtils.toString(response.getEntity)}")
    } else {
      EntityUtils.toString(response.getEntity)
    }
  }

  def api(method: String, url: String, body: String): String = {
    try {
      apiThrowException(method, url, body)
    } catch {
      case e: Throwable => {
        println(s"ERROR: ${e.toString}")
        return ""
      }
    }
  }

  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

  def dataFrameFromJSONString(jsonString: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dataset = spark.createDataset(jsonString :: Nil)
    spark.read.option("mode", "DROPMALFORMED").option("multiline", value = true).json(dataset)
  }
  def emptySchemaDataFrame(schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  def druidSQLAPI(query: String, host: String, resultFormat: String = "object", limit: Int = 10000): String = {
    // TODO: tech-debt, use proper spark druid connector when available, no official connector for this version of spark as of now
    val url = s"http://${host}:8888/druid/v2/sql"
    val requestBody = s"""{"resultFormat":"${resultFormat}","header":false,"context":{"sqlOuterLimit":${limit}},"query":"${query}"}"""
    api("POST", url, requestBody)
  }

  def druidDFOption(query: String, host: String, resultFormat: String = "object", limit: Int = 10000)(implicit spark: SparkSession): Option[DataFrame] = {
    val result = druidSQLAPI(query, host, resultFormat, limit).trim()
    // return empty data frame if result is an empty string
    if (result == "") {
      println(s"ERROR: druidSQLAPI returned empty string")
      return None
    }
    val df = dataFrameFromJSONString(result).persist(StorageLevel.MEMORY_ONLY)
    if (df.isEmpty) {
      println(s"ERROR: druidSQLAPI json parse result is empty")
      return None
    }
    // return empty data frame if there is an `error` field in the json
    if (hasColumn(df, "error")) {
      println(s"ERROR: druidSQLAPI returned error response, response=${result}")
      return None
    }
    // now that error handling is done, proceed with business as usual
    Some(df)
  }

  def cassandraTableAsDataFrame(keySpace: String, table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra")
      .option("inferSchema", "true")
      .option("keyspace", keySpace)
      .option("table", table)
      .load()
      .persist(StorageLevel.MEMORY_ONLY)
  }

  def postgresTableAsDataFrame(postgresUrl: String, tableName: String, dbUserName: String, dbCredential: String)(implicit spark: SparkSession): DataFrame = {
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", dbUserName)
    connectionProperties.setProperty("password", dbCredential)
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    spark.read.jdbc(postgresUrl, tableName, connectionProperties)
  }

  def saveDataframeToPostgresTable(df: DataFrame, dwPostgresUrl: String, tableName: String,
                                   dbUserName: String, dbCredential: String)(implicit spark: SparkSession): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("url", dwPostgresUrl)
      .option("dbtable", tableName)
      .option("user", dbUserName)
      .option("password", dbCredential)
      .option("driver", "org.postgresql.Driver")
      .format("jdbc")
      .save()
  }

  def elasticSearchDataFrame(host: String, index: String, query: String, fields: Seq[String], arrayFields: Seq[String] = Seq())(implicit spark: SparkSession): DataFrame = {
    var dfr = spark.read.format("org.elasticsearch.spark.sql")
      .option("es.read.metadata", "false")
      .option("es.nodes", host)
      .option("es.port", "9200")
      .option("es.index.auto.create", "false")
      .option("es.nodes.wan.only", "true")
      .option("es.nodes.discovery", "false")
    if (arrayFields.nonEmpty) {
      dfr = dfr.option("es.read.field.as.array.include", arrayFields.mkString(","))
    }
    dfr.option("query", query).load(index)
      .select(fields.map(f => col(f)):_*).persist(StorageLevel.MEMORY_ONLY) // select only the fields we need and persist
  }

  def saveDataframeToPostgresTable_With_Append(df: DataFrame, dwPostgresUrl: String, tableName: String,
                                               dbUserName: String, dbCredential: String)(implicit spark: SparkSession): Unit = {

    df.write
      .mode(SaveMode.Append)
      .option("url", dwPostgresUrl)
      .option("dbtable", tableName)
      .option("user", dbUserName)
      .option("password", dbCredential)
      .option("driver", "org.postgresql.Driver")
      .format("jdbc")
      .save()
  }

  def truncateWarehouseTable(table: String)(implicit spark: SparkSession, conf: DashboardConfig): Unit = {
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"
    val postgresProperties = new java.util.Properties()
    postgresProperties.setProperty("user", conf.dwPostgresUsername)
    postgresProperties.setProperty("password", conf.dwPostgresCredential)
    postgresProperties.setProperty("driver", "org.postgresql.Driver")

    val connection = java.sql.DriverManager.getConnection(dwPostgresUrl, postgresProperties)
    try {
      val statement = connection.createStatement()
      statement.executeUpdate(s"TRUNCATE TABLE ${table}")
    } finally {
      connection.close()
    }
  }

  def truncateWarehouseTable(table: String, dwPostgresUrl: String)(implicit spark: SparkSession, conf: DashboardConfig): Unit = {
    val postgresProperties = new java.util.Properties()
    postgresProperties.setProperty("user", conf.dwPostgresUsername)
    postgresProperties.setProperty("password", conf.dwPostgresCredential)
    postgresProperties.setProperty("driver", "org.postgresql.Driver")

    val connection = java.sql.DriverManager.getConnection(dwPostgresUrl, postgresProperties)
    try {
      val statement = connection.createStatement()
      statement.executeUpdate(s"TRUNCATE TABLE ${table}")
    } finally {
      connection.close()
    }
  }

  def mongodbTableAsDataFrame(mongoDatabase: String, collection: String)(implicit spark: SparkSession): DataFrame = {
    val schema = new StructType()
      .add("topiccount", IntegerType, true)
      .add("postcount", IntegerType, true)
      .add("sunbird-oidcId", StringType, true)
      .add("username", StringType, true)
    val df = spark.read.schema(schema).format("com.mongodb.spark.sql.DefaultSource").option("database", mongoDatabase).option("collection", collection).load()
    val filterDf = df.select("sunbird-oidcId").where(col("username").isNotNull or col("topiccount") > 0 and (col("postcount") > 0))
    val renamedDF = filterDf.withColumnRenamed("sunbird-oidcId", "userid")
    renamedDF
  }

  def mongodbSolutionsTableAsDataFrame(url: String, mongoDatabase: String, collection: String, solutionIdsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val schema = new StructType()
      .add("_id", StringType, true)
      .add("endDate", DateType, true)
    val df = spark.read.schema(schema)
      .format("com.mongodb.spark.sql.DefaultSource")
      .option("uri", url)
      .option("database", mongoDatabase)
      .option("collection", collection)
      .load()
    val cleanedDf = df.withColumn("_id_cleaned", regexp_replace(col("_id"), "[^0-9a-zA-Z]", ""))
    val solutionIds = solutionIdsDF.select("solutionIds").collect.map(_.getString(0))
    val filteredDf = cleanedDf.filter(col("_id_cleaned").isin(solutionIds: _*)).select(col("_id").alias("solutionIds"), col("endDate"))
    filteredDf.show(false)
    filteredDf
  }

  def mongodbReportConfigAsString(url: String, mongoDatabase: String, collection: String, filter: String)(implicit spark: SparkSession): String = {
    val schema = new StructType()
      .add("report", StringType, true)
      .add("config", StringType, true)
    val df = spark.read.schema(schema)
      .format("com.mongodb.spark.sql.DefaultSource")
      .option("uri", url)
      .option("database", mongoDatabase)
      .option("collection", collection)
      .load()
    val filteredDf = df.filter(col("report") === filter)
    val configAsString = filteredDf.select("config").first().getString(0)
    println(s"Report config for $filter \n " + configAsString)
    configAsString
  }

  def writeToCassandra(data: DataFrame, keyspace: String, table: String)(implicit spark: SparkSession): Unit = {
    data.write.format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .save()
  }


//  def checkAvailableColumns(df: DataFrame, expectedColumnsInput: List[String]) : DataFrame = {
//    expectedColumnsInput.foldLeft(df) {
//      (df, column) => {
//        if(!df.columns.contains(column)) {
//          df.withColumn(column, lit(null).cast(StringType))
//        } else df
//      }
//    }
//  }

  /**
   * return parsed int or zero if parsing fails
   * @param s string to parse
   * @return int or zero
   */
//  def intOrZero(s: String): Int = {
//    try {
//      s.toInt
//    } catch {
//      case e: Exception => 0
//    }
//  }

  /***
   * Validate if return value from one code block is equal to the return value from other block. Uses blocks so that the
   * spark code in blocks is only executed if validation=true
   *
   * @param msg info on what is being validated
   * @tparam T return type from the blocks
   * @throws AssertionError if values from blocks do not match
   */
  @throws[AssertionError]
  def validate[T](block1: => T, block2: => T, msg: String = ""): Unit = {
    if (validation) {
      val r1 = block1
      val r2 = block2
      if (r1.equals(r2)) {
        println(s"VALIDATION PASSED: ${msg}")
        println(s"  - value = ${r1}")
      } else {
        println(s"VALIDATION FAILED: ${msg}")
        println(s"  - value ${r1} does not equal value ${r2}")
        // throw new AssertionError("Validation Failed")
      }
    }
  }

  def show(df: DataFrame, msg: String = ""): Unit = {
    println("____________________________________________")
    println("SHOWING: " + msg)
    if (debug) {
      df.show()
      println("Count: " + df.count())
    }
    df.printSchema()
  }

  def showDS[T](ds: Dataset[T], msg: String = ""): Unit = {
    println("____________________________________________")
    println("SHOWING: " + msg)
    if (debug) {
      ds.show()
      println("Count: " + ds.count())
    }
    ds.printSchema()
  }
}


trait AbsDashboardModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    parseConfigAndProcessData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  def parseConfigAndProcessData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = DashboardConfigParser.parseConfig(config)
    if (conf.debug == "true") DashboardUtil.debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") DashboardUtil.validation = true // set validation to true if explicitly specified in the config
    if (DashboardUtil.debug) {
      println("Spark Config:")
      println(spark.conf.getAll)
    }
    // process
    processData(timestamp)
  }

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit
}

object Redis extends Serializable {

  var redisConnect: Jedis = null
  var redisHost: String = ""
  var redisPort: Int = 0
  var redisTimeout: Int = 30000

  // open/close redis connection
  def closeRedisConnect(): Unit = {
    if (redisConnect != null) {
      redisConnect.close()
      redisConnect = null
    }
  }
  def getOrCreateRedisConnect(host: String, port: Int): Jedis = {
    if (redisConnect == null) {
      redisConnect = createRedisConnect(host, port)
    } else if (redisHost != host || redisPort != port) {
      try {
        closeRedisConnect()
      } catch {
        case e: Exception => {}
      }
      redisConnect = createRedisConnect(host, port)
    }
    redisConnect
  }
  def getOrCreateRedisConnect(conf: DashboardConfig): Jedis = getOrCreateRedisConnect(conf.redisHost, conf.redisPort)
  def createRedisConnect(host: String, port: Int): Jedis = {
    redisHost = host
    redisPort = port
    if (host == "") return null
    new Jedis(host, port, redisTimeout)
  }
  def createRedisConnect(conf: DashboardConfig): Jedis = createRedisConnect(conf.redisHost, conf.redisPort)

  // get key value
  def get(key: String)(implicit conf: DashboardConfig): String = {
    get(conf.redisHost, conf.redisPort, conf.redisDB, key)
  }
  def get(host: String, port: Int, db: Int, key: String): String = {
    try {
      getWithoutRetry(host, port, db, key)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        getWithoutRetry(host, port, db, key)
    }
  }
  def getWithoutRetry(host: String, port: Int, db: Int, key: String): String = {
    if (key == null || key.isEmpty) {
      println(s"WARNING: key is empty")
      return "" // or any other default value
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping fetching the redis key=${key}")
      return "" // or any other default value
    }
    if (jedis.getDB != db) jedis.select(db)

    // Check if the key exists in Redis
    if (!jedis.exists(key)) {
      println(s"WARNING: Key=${key} does not exist in Redis")
      return "" // or any other default value
    }

    jedis.get(key)
  }

  // set key value
  def update(key: String, data: String)(implicit conf: DashboardConfig): Unit = {
    update(conf.redisHost, conf.redisPort, conf.redisDB, key, data)
  }
  def update(db: Int, key: String, data: String)(implicit conf: DashboardConfig): Unit = {
    update(conf.redisHost, conf.redisPort, db, key, data)
  }
  def update(host: String, port: Int, db: Int, key: String, data: String): Unit = {
    try {
      updateWithoutRetry(host, port, db, key, data)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        updateWithoutRetry(host, port, db, key, data)
    }
  }
  def updateWithoutRetry(host: String, port: Int, db: Int, key: String, data: String): Unit = {
    var cleanedData = ""
    if (data == null || data.isEmpty) {
      println(s"WARNING: data is empty, setting data='' for redis key=${key}")
      cleanedData = ""
    } else {
      cleanedData = data
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }
    if (jedis.getDB != db) jedis.select(db)
    jedis.set(key, cleanedData)
  }

  // get map field value
  def getMapField(key: String, field: String)(implicit conf: DashboardConfig): String = {
    getMapField(conf.redisHost, conf.redisPort, conf.redisDB, key, field)
  }
  def getMapField(host: String, port: Int, db: Int, key: String, field: String): String = {
    try {
      getMapFieldWithoutRetry(host, port, db, key, field)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        getMapFieldWithoutRetry(host, port, db, key, field)
    }
  }

  def getMapFieldWithoutRetry(host: String, port: Int, db: Int, key: String, field: String): String = {
    if (key == null || key.isEmpty) {
      println(s"WARNING: key is empty")
      return ""
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping fetching the redis key=${key}")
      return ""
    }
    if (jedis.getDB != db) jedis.select(db)

    // Check if the key exists in Redis
    if (!jedis.exists(key)) {
      println(s"WARNING: Key=${key} does not exist in Redis")
      return ""
    }

    // Fetch all fields and values from the Redis hash
    jedis.hget(key, field)
  }

  // set map field value
  def updateMapField(key: String, field: String, data: String)(implicit conf: DashboardConfig): Unit = {
    updateMapField(conf.redisHost, conf.redisPort, conf.redisDB, key, field, data)
  }
  def updateMapField(host: String, port: Int, db: Int, key: String, field: String, data: String): Unit = {
    try {
      updateMapFieldWithoutRetry(host, port, db, key, field, data)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        updateMapFieldWithoutRetry(host, port, db, key, field, data)
    }
  }
  def updateMapFieldWithoutRetry(host: String, port: Int, db: Int, key: String, field: String, data: String): Unit = {
    var cleanedData = ""
    if (data == null || data.isEmpty) {
      println(s"WARNING: data is empty, setting data='' for redis key=${key}")
      cleanedData = ""
    } else {
      cleanedData = data
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }
    if (jedis.getDB != db) jedis.select(db)
    jedis.hset(key, field, cleanedData)
  }

  // get map
  def getMapAsDataFrame(key: String, schema: StructType)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    getMapAsDataFrame(conf.redisHost, conf.redisPort, conf.redisDB, key, schema)
  }

  def getMapAsDataFrame(host: String, port: Int, db: Int, key: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    val data = getMap(host, port, db, key)
    if (data.isEmpty) {
      DashboardUtil.emptySchemaDataFrame(schema)
    } else {
      spark.createDataFrame(data.map(r => Row(r._1, r._2)).toList, schema)
    }
  }

  def getMap(key: String)(implicit conf: DashboardConfig): util.Map[String, String] = {
    getMap(conf.redisHost, conf.redisPort, conf.redisDB, key)
  }

  def getMap(host: String, port: Int, db: Int, key: String): util.Map[String, String] = {
    try {
      getMapWithoutRetry(host, port, db, key)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        getMapWithoutRetry(host, port, db, key)
    }
  }

  def getMapWithoutRetry(host: String, port: Int, db: Int, key: String): util.Map[String, String] = {
    if (key == null || key.isEmpty) {
      println(s"WARNING: key is empty")
      return new util.HashMap[String, String]()
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping fetching the redis key=${key}")
      return new util.HashMap[String, String]()
    }
    if (jedis.getDB != db) jedis.select(db)

    // Check if the key exists in Redis
    if (!jedis.exists(key)) {
      println(s"WARNING: Key=${key} does not exist in Redis")
      return new util.HashMap[String, String]()
    }

    // Fetch all fields and values from the Redis hash
    jedis.hgetAll(key)
  }

  // set map, replace all keys
  def dispatch(key: String, data: util.Map[String, String], replace: Boolean = true)(implicit conf: DashboardConfig): Unit = {
    dispatch(conf.redisHost, conf.redisPort, conf.redisDB, key, data, replace)
  }
  def dispatch(db: Int, key: String, data: util.Map[String, String], replace: Boolean)(implicit conf: DashboardConfig): Unit = {
    dispatch(conf.redisHost, conf.redisPort, db, key, data, replace)
  }
  def dispatch(host: String, port: Int, db: Int, key: String, data: util.Map[String, String], replace: Boolean): Unit = {
    try {
      dispatchWithoutRetry(host, port, db, key, data, replace)
    } catch {
      case e: JedisException =>
        redisConnect = createRedisConnect(host, port)
        dispatchWithoutRetry(host, port, db, key, data, replace)
    }
  }
  def dispatchWithoutRetry(host: String, port: Int, db: Int, key: String, data: util.Map[String, String], replace: Boolean): Unit = {
    if (data == null || data.isEmpty) {
      println(s"WARNING: map is empty, skipping saving to redis key=${key}")
      return
    }
    val jedis = getOrCreateRedisConnect(host, port)
    if (jedis == null) {
      println(s"WARNING: jedis=null means host is not set, skipping saving to redis key=${key}")
      return
    }
    if (jedis.getDB != db) jedis.select(db)
    if (replace) {
      replaceMap(jedis, key, data)
    } else {
      jedis.hset(key, data)
    }
  }

  def getHashKeys(jedis: Jedis, key: String): Seq[String] = {
    val keys = ListBuffer[String]()
    val scanParams = new ScanParams().count(100)
    var cur = ScanParams.SCAN_POINTER_START
    do {
      val scanResult = jedis.hscan(key, cur, scanParams)
      scanResult.getResult.foreach(res => {
        keys += res.getKey
      })
      cur = scanResult.getCursor
    } while (!cur.equals(ScanParams.SCAN_POINTER_START))
    keys.toList
  }

  def replaceMap(jedis: Jedis, key: String, data: util.Map[String, String]): Unit = {
    // this deletes the keys that do not exist anymore manually
    val existingKeys = getHashKeys(jedis, key)
    val toDelete = existingKeys.toSet.diff(data.keySet())
    if (toDelete.nonEmpty) jedis.hdel(key, toDelete.toArray:_*)
    // this will update redis hash map keys and create new ones, but will not delete ones that have been deleted
    jedis.hset(key, data)
  }

  /**
   * Convert data frame into a map, and save to redis
   *
   * @param redisKey key to save df data to
   * @param df data frame
   * @param keyField column name that forms the key (must be a string)
   * @param valueField column name that forms the value
   * @tparam T type of the value column
   */
  def dispatchDataFrame[T](redisKey: String, df: DataFrame, keyField: String, valueField: String, replace: Boolean = true)(implicit conf: DashboardConfig): Unit = {
    import DashboardUtil._
    dispatch(redisKey, df.toMap[T](keyField, valueField), replace)
  }

}
