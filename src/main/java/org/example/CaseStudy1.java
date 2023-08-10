package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.awt.*;

import static org.apache.spark.sql.functions.*;

/*
The requirement is to create a report:

1. That contains the aggregate (sum) Profit and Quantity, based on Year, Month, Category, Sub Category.
So your report file should contain:
Year, Month, Category, Sub Category, Total Quantity Sold, Total Profit

2. This data has to be stored in a partition based on year month. like year=2014/month=11

3. For the total profit and total quantity calculations the returns data should be used to exclude all returns

4. There must be at least one unit test case

5. This program must be runnable with any input directories as parameters to read the sales and returns data

Try to write optimized spark operations.
 */
public class CaseStudy1 {
    public static void main(String[] args) {
        String winutilPath = "C:\\Users\\Manjula\\OneDrive\\Documents\\software\\spark-3.4.1-bin-hadoop3"; //\\bin\\winutils.exe"; //bin\\winutils.exe";
//        System.setProperty("hadoop.home.dir", winutilPath);
//        System.setProperty("HADOOP_HOME", winutilPath);
//        if(System.getProperty("os.name").toLowerCase().contains("win")) {
//            System.out.println("Detected windows");
//            System.setProperty("hadoop.home.dir", winutilPath);
//            System.setProperty("HADOOP_HOME", winutilPath);
//        }

        String INPUT_PATH_SALES="C:\\Users\\Manjula\\OneDrive\\Documents\\abhi\\Jigsaw L2\\case study 1\\dataset\\Global Superstore Sales - Global Superstore Sales.csv";
        String INPUT_PATH_RETURNS="C:\\Users\\Manjula\\OneDrive\\Documents\\abhi\\Jigsaw L2\\case study 1\\dataset\\Global Superstore Sales - Global Superstore Returns.csv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> salesDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(INPUT_PATH_SALES);

        Dataset<Row> returnsDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(INPUT_PATH_RETURNS);





        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

        Dataset<Row> cleanedSalesDf =
                salesDf.withColumn("Year",split(col("Order Date"), "[-/]").getItem(2))
                        .withColumn("Month",split(col("Order Date"), "[-/]").getItem(0))
                        .withColumn("Profit_num", regexp_replace(col("Profit"),"[$]","").cast("int"))
                        .select("Year","Month","Order ID","Category", "Sub-Category","Profit_num","Quantity");

        cleanedSalesDf.printSchema();

        cleanedSalesDf.createOrReplaceTempView("SALES");
        returnsDf.createOrReplaceTempView("RETURNS");

        Dataset<Row> joinDF = spark.sql(
                "SELECT s.*,Returned FROM SALES s LEFT OUTER JOIN RETURNS r ON s.`Order ID` == r.`Order ID` where Returned is null"
        );

        joinDF.printSchema();

        WindowSpec windowSpec = Window.partitionBy("Year", "Month", "Category", "Sub-Category");
        Dataset<Row> reportDf = joinDF.withColumn("Total Profit", functions.sum("Profit_num").over(windowSpec))
                .withColumn("Total Quantity Sold", functions.sum("Quantity").over(windowSpec))
                .select("Year", "Month", "Category", "Sub-Category","Total Quantity Sold","Total Profit")
                .dropDuplicates();

        System.out.println("count: "+reportDf.count());

        reportDf.write().option("header","true")
        .partitionBy("Year", "Month")
        .mode("overwrite")
        .csv("C:/Users/Manjula/OneDrive/Documents/abhi/Jigsaw L2/case study 1/output");
    }

}
