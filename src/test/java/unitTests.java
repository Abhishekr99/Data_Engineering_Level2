
import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Sequence;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.CaseStudy1;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.example.CaseStudy1.aggregateProfitAndQuantity;
import static org.example.CaseStudy1.cleanDf;

public class unitTests {
    private static SparkSession spark;
    static String salesRows, returnsRows, salesHeader, returnHeader;

    @BeforeClass
    public static void setUp() {

        spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        salesHeader="Order ID,Order Date,Ship Mode,Segment,City,State,Country,Category,Sub-Category,Sales,Quantity,Discount,Profit,Shipping Cost,Returns";
        returnHeader="Order ID,Returned";

        salesRows=
                "CA-2014-AB10015140-41954,11/11/2014,First Class,Consumer,Oklahoma City,Oklahoma,United States,Technology,Phones,$221.98,2,0,$62.15,40.77,No\n" +
                "IN-2014-JR162107-41675,2/5/2014,Second Class,Corporate,Wollongong,New South Wales,Australia,Furniture,Chairs,$3709.40,9,0.1,-$288.77,923.63,No\n" +
                "CA-2014-AB10015140-41954,11/11/2014,First Class,Consumer,Oklahoma City,Oklahoma,United States,Technology,Phones,$341.96,2,0,$54.71,25.27,No\n" +
                "IN-2014-JR162107-41674,4/18/2014,First Class,Consumer,Sydney,New South Wales,Australia,Technology,Copiers,$1601.64,5,0.1,$587.19,511.47,Yes";

        returnsRows="IN-2014-JR162107-41675,Yes";


    }

    @AfterClass
    public static void tearDown() {
        spark.stop();
    }

    StructType createSchema(String... columns){
        List<StructField> fields = new ArrayList<>();
        for (String column : columns) {
            fields.add(createStructField(column, StringType, false));
        }
        return DataTypes.createStructType(fields);
    }

    Dataset<Row> createDataset(String rows, StructType schema){
        List<Row> rowList=new ArrayList<Row>();

        for(String row: rows.split("\n")){
            rowList.add(RowFactory.create(row.split(",")));
        }
        Dataset<Row> dataset = spark.sqlContext().createDataFrame(rowList, schema);
        return dataset;
    }


    @Test
    public void testDataCleaning() {

        StructType salesSchema = createSchema(salesHeader.split(","));
        Dataset<Row> salesDf = createDataset(salesRows, salesSchema);

        Dataset<Row> cleanedSalesDf = cleanDf(salesDf);

        List<Double> cleanedProfitCol = new ArrayList<>();
        for(Row row: cleanedSalesDf.select("Profit_num").collectAsList()){
            cleanedProfitCol.add((Double) row.get(0));
        }

        List<Double> expectedProfitCol = Arrays.asList(62.15,-288.77,54.71,587.19);
        Assert.assertEquals(expectedProfitCol, cleanedProfitCol);
        Assert.assertTrue(Arrays.asList(cleanedSalesDf.columns()).contains("Year"));
        Assert.assertTrue(Arrays.asList(cleanedSalesDf.columns()).contains("Month"));


    }

    @Test
    public void testAggregation(){
        StructType returnsSchema = createSchema(returnHeader.split(","));
        Dataset<Row> returnsDf = createDataset(returnsRows, returnsSchema);

        StructType salesSchema = createSchema(salesHeader.split(","));
        Dataset<Row> salesDf = createDataset(salesRows, salesSchema);

        Dataset<Row> cleanedSalesDf =cleanDf(salesDf);
        cleanedSalesDf.createOrReplaceTempView("SALES");
        returnsDf.createOrReplaceTempView("RETURNS");
        Dataset<Row> joinDF = spark.sql(
                "SELECT s.*,r.Returned FROM SALES s LEFT OUTER JOIN RETURNS r ON s.`Order ID` == r.`Order ID` where Returned is null"
        );

        WindowSpec windowSpec = Window.partitionBy("Year", "Month", "Category", "Sub-Category");
        Dataset<Row> reportDf = aggregateProfitAndQuantity(joinDF, windowSpec);

        List<Double> totalProfits = new ArrayList<>();
        List<Double> totalQty = new ArrayList<>();
        for(Row row: reportDf.select("Total Profit", "Total Quantity Sold").collectAsList()){
            totalProfits.add((Double) row.get(0));
            totalQty.add((Double) row.get(1));
        }
//        double expectedProfit = reportDf.select("Total Profit").collectAsList().get(0).getDouble(0);
        Assert.assertEquals(Arrays.asList(116.86,587.19), totalProfits);
        Assert.assertEquals(Arrays.asList(4.0,5.0), totalQty);
    }

}
