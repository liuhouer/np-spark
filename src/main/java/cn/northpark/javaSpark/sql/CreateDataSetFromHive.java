package cn.northpark.javaSpark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//-server -Xms512M -Xmx1024M -XX:PermSize=256M -XX:MaxNewSize=512M -XX:MaxPermSize=512M
public class CreateDataSetFromHive {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkHive")
//                .master("local")
//                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("hive.metastore.uris","thrift://c7node1:9083")
                .enableHiveSupport()
                .getOrCreate();

//        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
//        spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");
        Dataset<Row> sql = spark.sql("SELECT * FROM src");
//                sql.show();
    }
}
