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

    public static Dataset<Row> readCsv(SparkSession spark, String INPUT_PATH){
        Dataset<Row> inputDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(INPUT_PATH);
        return  inputDf;
    }
    public static Dataset<Row> cleanDf(Dataset<Row> df){
        Dataset<Row> cleanedDf =
                df.withColumn("Year",split(col("Order Date"), "[-/]").getItem(2))
                        .withColumn("Month",split(col("Order Date"), "[-/]").getItem(0))
                        .withColumn("Profit_num", regexp_replace(col("Profit"),"[$]","").cast("Double"))
                        .select("Year","Month","Order ID","Category", "Sub-Category","Profit_num","Quantity");
        return cleanedDf;
    }

    public static Dataset<Row> aggregateProfitAndQuantity(Dataset<Row> df, WindowSpec windowSpec){
        return
                df.withColumn("Total Profit", functions.sum("Profit_num").over(windowSpec))
                        .withColumn("Total Quantity Sold", functions.sum("Quantity").over(windowSpec))
                        .select("Year", "Month", "Category", "Sub-Category","Total Quantity Sold","Total Profit")
                        .dropDuplicates();
    }

    static void writeCsv(Dataset<Row> df, String OUTPUT_PATH, String... PartitionColumns){
        df.write().option("header","true")
                .partitionBy(PartitionColumns)
                .mode("overwrite")
                .csv(OUTPUT_PATH);

    }

    public static void main(String[] args) {

        String INPUT_PATH_SALES=args[0];
        String INPUT_PATH_RETURNS=args[1];
        String OUTPUT_PATH=args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        //read csv source files
        Dataset<Row> salesDf = readCsv(spark,INPUT_PATH_SALES);
        Dataset<Row> returnsDf = readCsv(spark,INPUT_PATH_RETURNS);

        //clean source data
        Dataset<Row> cleanedSalesDf =cleanDf(salesDf);

        //creating temp view
        cleanedSalesDf.createOrReplaceTempView("SALES");
        returnsDf.createOrReplaceTempView("RETURNS");

        //joining sales & returns data
        Dataset<Row> joinDF = spark.sql(
                "SELECT s.*,Returned FROM SALES s LEFT OUTER JOIN RETURNS r ON s.`Order ID` == r.`Order ID` where Returned is null"
        );

        //aggregation window
        WindowSpec windowSpec = Window.partitionBy("Year", "Month", "Category", "Sub-Category");

        //aggregation for report
        Dataset<Row> reportDf = aggregateProfitAndQuantity(joinDF, windowSpec);

        //writing output partitioned by (year,month)
        writeCsv(reportDf,OUTPUT_PATH,"Year", "Month");

    }

}
