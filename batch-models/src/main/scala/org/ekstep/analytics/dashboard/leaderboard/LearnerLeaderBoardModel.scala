package org.ekstep.analytics.dashboard.leaderboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext

object LearnerLeaderBoardModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.leaderboard.LearnerLeaderBoardModel"

  override def name() = "LearnerLeaderBoardModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // get previous month start and end dates
    val monthStart = date_format(date_trunc("MONTH", add_months(current_date(), -1)), dateTimeFormat)
    val monthEnd = date_format(last_day(add_months(current_date(), -1)), dateFormat+" 23:59:59")

    //get previous month and year values
    val (month, year) = (
      date_format(date_add(last_day(add_months(current_date(), -1)), 1), "M"),
      date_format(add_months(current_date(), -1), "yyyy")
    )

    // Aggregate karma points data
    val karmaPointsDataDF = cache.load("userKarmaPoints")
      .filter(col("credit_date") >= monthStart && col("credit_date") <= monthEnd)
      .groupBy(col("userid")).agg(sum(col("points")).alias("total_points"), max(col("credit_date")).alias("last_credit_date")).cache()

    show(karmaPointsDataDF, "this is the kp_data")

    // Broadcast small DataFrames
    val adminUsersDF = broadcast(roleDataFrame()
      .groupBy("userID")
      .agg(concat_ws(", ", collect_list("role")).alias("role"))
      .filter(col("role").contains("MDO_ADMIN")))

    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // get org_ids who have atleast one MDO_ADMIN
    val orgWithAtleastOneMdoAdmin = userOrgDF
      .join(broadcast(adminUsersDF), "userID")
      .select(userOrgDF("userOrgID"))
      .distinct()
    show(orgWithAtleastOneMdoAdmin, "orgs with atleast 1 mdoAdmin")

    // get org_ids with atleast 'n' user [n=5 for preprod and 10 for prod]
    val orgWithMoreThanNusersDF = userOrgDF
      .groupBy("userOrgID")
      .agg(count("userID").alias("count"))
      .filter(col("count") > 10)
      .select("userOrgID")
    show(orgWithMoreThanNusersDF, "orgs with more than (n=10) users")


    // as per suresh's suggestion, removing the condition of orgs with atleast 1 mdoAdmin
    val commonOrgIdsDF = orgWithMoreThanNusersDF.cache()
//    // get intersection of orgs (with atleast one mdo admin and with more than n user)
//    val commonOrgIdsDF = orgWithAtleastOneMdoAdmin.intersect(orgWithMoreThanNusersDF).cache()
    show(commonOrgIdsDF, "commonOrgs satisfying the condition")

    //fetch the users from the above mentioned orgs only
    val filteredUserOrgDF = userOrgDF.join(commonOrgIdsDF, "userOrgID")
    show(filteredUserOrgDF, "filterUserOrg")

    // fetch user details like fullname, profileImg etc for users of selected orgs
    val userOrgData = filteredUserOrgDF.withColumnRenamed("fullName","full_Name")
      .join(userDF.select("userID", "fullName", "userProfileImgUrl"), "userID")
      .select(
        filteredUserOrgDF("userID").alias("userid"),
        filteredUserOrgDF("userOrgID").alias("org_id"),
        filteredUserOrgDF("fullName").alias("fullname"),
        filteredUserOrgDF("userProfileImgUrl").alias("profile_image"))
      .cache()

    //join karma points details with user details and select required columns
    var userLeaderBoardDataDF = userOrgData.join(karmaPointsDataDF, Seq("userid"), "left")
      .filter(col("org_id") =!= "")
      .select(userOrgData("userid"),
        userOrgData("org_id"),
        userOrgData("fullname"),
        userOrgData("profile_image"),
        karmaPointsDataDF("total_points"),
        karmaPointsDataDF("last_credit_date"))
      .withColumn("month", (month - 1).cast("int"))
      .withColumn("year", lit(year))
    show(userLeaderBoardDataDF, "finaluserdata")

    val windowSpecRank = Window.partitionBy("org_id").orderBy(desc("total_points"))

    // rank the users based on the points within each org
    userLeaderBoardDataDF = userLeaderBoardDataDF.withColumn("rank", dense_rank().over(windowSpecRank))

    // sort them based on their fullNames for each rank group within each org
    val windowSpecRow = Window.partitionBy("org_id").orderBy(col("rank"), col("last_credit_date").desc)
    userLeaderBoardDataDF = userLeaderBoardDataDF.withColumn("row_num", row_number().over(windowSpecRow))

    //read existing leaderboard data from cassandra to fetch ranks and update the previous rank column in new dataframe
    val learnerLeaderboardDF = cache.load("learnerLeaderBoard")

    // final df for writing to cassandra learner leaderboard table
    val finalUserLeaderBoardDF = userLeaderBoardDataDF
      .join(learnerLeaderboardDF, Seq("userid"), "left_outer")
      .select(
        userLeaderBoardDataDF("org_id"),
        userLeaderBoardDataDF("userid"),
        userLeaderBoardDataDF("total_points"),
        userLeaderBoardDataDF("rank"),
        userLeaderBoardDataDF("row_num"),
        userLeaderBoardDataDF("fullname"),
        userLeaderBoardDataDF("profile_image"),
        userLeaderBoardDataDF("month"),
        userLeaderBoardDataDF("year"),
        coalesce(learnerLeaderboardDF("rank"), lit(0)).alias("previous_rank") // Use coalesce to handle null ranks
      )

    // write to cassandra learner_leaderboard and lookup tables respectively
    writeToCassandra(finalUserLeaderBoardDF,conf.cassandraUserKeyspace, conf.cassandraLearnerLeaderBoardTable)
    writeToCassandra(finalUserLeaderBoardDF.select("userid", "row_num"),conf.cassandraUserKeyspace, conf.cassandraLearnerLeaderBoardLookupTable)
  }
}