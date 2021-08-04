import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.log4j.Logger
import org.apache.log4j.Level

case class Record(key: Int, value: String)

object Project1 {

    def main(args: Array[String]) {

        val spark = SparkSession
        .builder()
        .config("spark.master", "local[*]")
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
        .config("spark.sql.hive.metastore.jars", "/usr/local/Cellar/hive/3.1.2/lib/*")
        .config("spark.sql.hive.metastore.version", "3.1")
        .appName("Project 1")
        .enableHiveSupport()
        .getOrCreate()

        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
                
        import spark.implicits._
        import spark.sql

        sql("USE project1")
        sql("SELECT * FROM sbux").show(Int.MaxValue)

        sql("DESCRIBE FORMATTED sbux").show(Int.MaxValue)

        // Scenario 1
        // What is the total number of consumers for Branch1?
        println("Scenario 1:")
        println("What is the total number of consumers for Branch1?")
        sql("SELECT SUM(bev_count) FROM sbux WHERE branch_name='Branch1'").show()
        println()

        // What is the number of consumers for the Branch2?
        println("What is the number of consumers for the Branch2?")
        sql("SELECT SUM(bev_count) FROM sbux WHERE branch_name='Branch2'").show()
        println()

        // Scenario 2
        // What is the most consumed beverage on Branch1?
        println("Scenario 2:")
        println("What is the most consumed beverage on Branch1?")
        sql("SELECT bev_name, bev_count FROM (SELECT bev_name, bev_count, RANK () OVER (ORDER BY bev_count DESC) cnt_rank FROM sbux WHERE branch_name='Branch1') AS ranked WHERE ranked.cnt_rank=1").show()
        println()

        // What is the least consumed beverage on Branch2?
        println("What is the least consumed beverage on Branch2?")
        sql("SELECT bev_name, bev_count FROM (SELECT bev_name, bev_count, RANK () OVER (ORDER BY bev_count) cnt_rank FROM sbux WHERE branch_name='Branch2') AS ranked WHERE ranked.cnt_rank=1").show()
        println()

        // What is the average consumed beverage of Branch2? (Median)
        println("What is the average consumed beverage of Branch2?")
        sql("SELECT bev_name, bev_count FROM (SELECT bev_name, bev_count, CUME_DIST () OVER (ORDER BY bev_count) cnt_frac FROM sbux WHERE branch_name='Branch2') AS ranked WHERE ranked.cnt_frac >= 0.5 LIMIT 1").show()
        println()

        // Scenario 3
        // What are the beverages available on Branch10, Branch8, and Branch1?
        println("Scenario 3:")
        println("What are the beverages available on Branch10, Branch8, and Branch1?")
        sql("SELECT DISTINCT bev_name FROM sbux WHERE branch_name='Branch1' OR branch_name='Branch8' OR branch_name='Branch10'").show(Int.MaxValue)
        println()

        // What are the common beverages available in Branch4, Branch7?
        println("What are the common beverages available in Branch4, Branch7?")
        sql("SELECT DISTINCT bev_name FROM sbux WHERE branch_name='Branch4' INTERSECT SELECT DISTINCT bev_name FROM sbux WHERE branch_name='Branch7'").show(Int.MaxValue)
        println()

        spark.stop()
    }
}