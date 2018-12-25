import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class readjson {
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("online_car-hailing");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        new readjson().readFile(jssc.sparkContext(), "passenger");
        new readjson().readFile(jssc.sparkContext(), "driver");
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public Dataset readFile(JavaSparkContext sc, String type){
        SQLContext sqlContext = new SQLContext(sc);
        //Dataset dataset = sqlContext.jsonFile("info");
        Dataset dataset = sqlContext.load(type+"-info").toDF();
        dataset.show();
        //Dataset<Row> dataset = sqlContext.read.format("parquet").load("路径");
        return  dataset;
    }
}
