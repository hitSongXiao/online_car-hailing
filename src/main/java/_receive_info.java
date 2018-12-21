import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class _receive_info {

    public static void main(String args[]){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("online_car-hailing");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        //lines.persist().print();//dstream().saveAsObjectFiles(".//resource/data","obj");
        JavaDStream<Location> info = lines.map(line->{
            String value[] = line.split("\\s+");
            double longitude = Double.valueOf(value[1].split(":")[1]);
            double latitude = Double.valueOf(value[2].split(":")[1]);
            String typeId[] = value[0].split(":");
            Location loc = new Location(typeId[0],longitude, latitude,Integer.valueOf(typeId[1]));
            return loc;
        });
        info.print();
        //new _receive_info().savePassengerFile(jssc.sparkContext(),info.filter(location -> location.getType().equals("passenger")));
        //new _receive_info().saveDriverFile(jssc.sparkContext(),info.filter(location -> location.getType().equals("driver")));
        new _receive_info().readFile(jssc.sparkContext(), "passenger").show();
        new SQLContext(SparkSession.builder().config(conf).getOrCreate());
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void savePassengerFile(JavaSparkContext sc,JavaDStream<Location> dstream){
        //sc.textFile("//data/passenger.txt").map(.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF();
        dstream.foreachRDD((JavaRDD<Location>rdd)->{
            if(!rdd.isEmpty()){
                SparkSession session = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
                Dataset<Row> df = session.createDataFrame(rdd, Location.class);
                df.write().mode(SaveMode.Append).save("passenger-info");
            }
        });
    }

    public void saveDriverFile(JavaSparkContext sc,JavaDStream<Location> dstream){
        //sc.textFile("//data/passenger.txt").map(.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF();
        dstream.foreachRDD((JavaRDD<Location>rdd)->{
            if(!rdd.isEmpty()){
                SparkSession session = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
                Dataset<Row> df = session.createDataFrame(rdd, Location.class);
                df.write().mode(SaveMode.Append).save("driver-info");
            }
        });
    }


    public void match(JavaPairDStream<Integer,Location>passenger,JavaPairDStream<Integer,Location>driver){
        JavaPairDStream<Integer, Tuple2<Location,Location>>areaInfo = passenger.join(driver);

    }

    public Dataset readFile(JavaSparkContext sc, String type){
        SQLContext sqlContext = new SQLContext(sc);
        //Dataset dataset = sqlContext.jsonFile("info");
        Dataset dataset = sqlContext.load("info").toJSON();
        //dataset.show();
        //Dataset<Row> dataset = sqlContext.read.format("parquet").load("路径");
        return  dataset;
    }

}

