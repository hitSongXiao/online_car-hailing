import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class _receive_info {
    public static void main(String args[]){
        String passenger = "passenger";
        String driver = "driver";
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("online_car-hailing");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        //lines.persist().print();//dstream().saveAsObjectFiles(".//resource/data","obj");
        JavaPairDStream<String, Location>info = lines.mapToPair(line->{
            String value[] = line.split("\\s+");
            double longitude = Double.valueOf(value[1].split(":")[1]);
            double latitude = Double.valueOf(value[2].split(":")[1]);
            Location loc = new Location(longitude, latitude);
            return new Tuple2<>(value[0],loc);
        });
        info.print();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
