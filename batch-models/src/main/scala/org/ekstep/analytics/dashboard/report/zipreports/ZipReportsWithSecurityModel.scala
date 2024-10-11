package org.ekstep.analytics.dashboard.report.zipreports

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import net.lingala.zip4j.ZipFile
import org.apache.commons.io.FileUtils
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.EncryptionMethod
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import java.io.{File}
import java.nio.file.{Files, Paths, StandardCopyOption}


/**
 * Model for processing the operational reports into a single password protected zip file
 */
object ZipReportsWithSecurityModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.zipreports.ZipReportsWithSecurityModel"
  override def name() = "ZipReportsWithSecurityModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // Start of merging folders

    // Define variables for source, destination directories and date.
    val prefixDirectoryPath = s"${conf.localReportDir}/${conf.prefixDirectoryPath}"
    val destinationPath = s"${conf.localReportDir}/${conf.destinationDirectoryPath}"
    val directoriesToSelect = conf.directoriesToSelect.split(",").toSet
    val specificDate = getDate()
    val kcmFolderPath = s"${conf.localReportDir}/${conf.kcmReportPath}/${specificDate}/ContentCompetencyMapping"

    // Method to traverse all the report folders within the source folder and check for specific date folder
    def traverseDirectory(directory: File): Unit = {
      // Get the list of files and directories in the current directory
      val files = directory.listFiles().filter(file => directoriesToSelect.contains(file.getName))
      if (files != null) {
        // Iterate over each file or directory
        for (file <- files) {
          // If it's a directory, recursively traverse it
          if (file.isDirectory) {
            // Check if the current directory contains the specific date folder
            val dateFolder = new File(file, specificDate)
            if (dateFolder.exists() && dateFolder.isDirectory) {
              // Inside the specific date folder, iterate over mdoid folders
              traverseDateFolder(dateFolder)
            }
          }
        }
      }
    }

    // Method to traverse the date folder and its subdirectories in each report folder
    def traverseDateFolder(dateFolder: File): Unit = {
      // Get the list of files and directories in the date folder
      val files = dateFolder.listFiles()
      if (files != null) {
        // Iterate over mdoid folders
        for (mdoidFolder <- files if mdoidFolder.isDirectory) {
          // Inside each mdoid folder, copy all CSV files to the destination directory
          copyFiles(mdoidFolder, destinationPath)
          // Copy the competencyMapping file from kcm-report folder to the destination mdoid folder
          copyKCMFile(mdoidFolder)
        }
      }
    }

    // Method to copy all CSV files inside an mdoid folder to the destination directory
    def copyFiles(mdoidFolder: File, destinationPath: String): Unit = {
      // Create destination directory if it does not exist
      val destinationDirectory = Paths.get(destinationPath, mdoidFolder.getName)
      if (!Files.exists(destinationDirectory)) {
        Files.createDirectories(destinationDirectory)
      }
      // Get the list of CSV files in the mdoid folder
      val csvFiles = mdoidFolder.listFiles().filter(file => file.getName.endsWith(".csv"))
      if (csvFiles != null) {
        // Copy all CSV files to the destination directory
        for (csvFile <- csvFiles) {
          val destinationFile = Paths.get(destinationDirectory.toString, csvFile.getName)
          Files.copy(csvFile.toPath, destinationFile, StandardCopyOption.REPLACE_EXISTING)
        }
      }
    }

    // Method to copy the desired file from kcm-report folder to the destination mdoid folder
    def copyKCMFile(mdoidFolder: File): Unit = {
      val kcmFile = new File(kcmFolderPath, "ContentCompetencyMapping.csv")
      val destinationDirectory = Paths.get(destinationPath, mdoidFolder.getName)
      if (kcmFile.exists() && destinationDirectory.toFile.exists()) {
        val destinationFile = Paths.get(destinationDirectory.toString, kcmFile.getName)
        Files.copy(kcmFile.toPath, destinationFile, StandardCopyOption.REPLACE_EXISTING)
      }
    }

    // Start traversing the source directory
    traverseDirectory(new File(prefixDirectoryPath))

    // End of merging folders

    // Start of zipping the reports and syncing to blob store
    // Define variables for source, blobStorage directories and password.
    val password = conf.password
    // Traverse through source directory to create individual zip files (mdo-wise)
    val mdoidFolders = new File(destinationPath).listFiles()
    if (mdoidFolders != null) {
      mdoidFolders.foreach { mdoidFolder =>
        if (mdoidFolder.isDirectory) { // Check if it's a directory
          val zipFilePath = s"${mdoidFolder}"
          // Create a password-protected zip file for the mdoid folder
          val zipFile = new ZipFile(zipFilePath+"/reports.zip", password.toCharArray)
          val parameters = new ZipParameters()
          parameters.setEncryptFiles(true)
          parameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD)
          // Add all files within the mdoid folder to the zip file
          mdoidFolder.listFiles().foreach { file =>
            zipFile.addFile(file, parameters)
          }
          // Delete the csvs keeping only the zip file from mdo folders
          mdoidFolder.listFiles().foreach { file =>
            print("Deleting csvs withing this: " +mdoidFolder)
            if (file.getName.toLowerCase.endsWith(".csv")) {
              file.delete()
            }
          }
          // Upload the zip file to blob storage
          val mdoid = mdoidFolder.getName

          //sync reports
          val zipReporthPath = s"${conf.destinationDirectoryPath}/$mdoid"
          syncReports(zipFilePath, zipReporthPath)

          println(s"Password-protected ZIP file created and uploaded for $mdoid: $zipFilePath")
        }
      }
    } else {
      println("No mdoid folders found in the given directory.")
    }
    // End of zipping the reports and syncing to blob store
    //deleting the tmp merged folder
    try {
      FileUtils.deleteDirectory(new File(destinationPath))
    } catch {
      case e: Exception => println(s"Error deleting directory: ${e.getMessage}")
    }
  }
}