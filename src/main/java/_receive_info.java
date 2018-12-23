import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.api.java.StorageLevels.MEMORY_AND_DISK_SER;

public class _receive_info {

    public static void main(String args[]){
        _receive_info receive = new _receive_info();
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("online_car-hailing");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        //lines.persist().print();//dstream().saveAsObjectFiles(".//resource/data","obj");
        JavaPairDStream<String,Location> info = lines.mapToPair(line->{
            String value[] = line.split("\\s+");
            double longitude = Double.valueOf(value[1].split(":")[1]);
            double latitude = Double.valueOf(value[2].split(":")[1]);
            String typeId[] = value[0].split(":");
            Location loc = new Location(typeId[0],longitude, latitude,Integer.valueOf(typeId[1]));
            return new Tuple2<>(value[0],loc);
        });
        info.persist().persist(MEMORY_AND_DISK_SER);
        JavaPairDStream<String,Location>passengers = info.filter((tuple)->(tuple._1().split(":")[0].equals("passenger")));
        JavaPairDStream<String,Location>drivers = info.filter((tuple)->(tuple._1().split(":")[0].equals("driver")));
        JavaPairDStream<String,Location>driver = info.filter((tuple)-> (tuple._1().split(":")[0].equals("driver")));
        JavaPairDStream<Integer, Map<String,Location>> passengers_each_area = receive.mapToArea2(passengers);
        JavaPairDStream<Integer, Map<String,Location>> drivers_each_area = receive.mapToArea2(drivers);
        Function3<Integer, Optional<Map<String,Location>>, State<Map<String,Location>>,Map<String,Location>> function = (word, op, state)->{
            if(!state.exists()||op.isPresent()){
                state.update(op.get());
            }
            else{
                Map<String,Location> locs = state.get();
                locs.putAll(op.get());
                state.update(locs);
            }
            return state.get();
        };
        JavaPairDStream<Integer, Map<String,Location>> passenger_info = passengers_each_area.mapWithState(StateSpec.function(function)).stateSnapshots();
        JavaPairDStream<Integer, Map<String,Location>> driver_info = drivers_each_area.mapWithState(StateSpec.function(function)).stateSnapshots();
        //passenger_info.print();
        //driver_info.print();
        for(int i = 0;i<9;i++){
            JavaPairDStream<Integer,Tuple2<Map<String,Location>,Map<String,Location>>> pair = receive.match(passenger_info, driver_info);
            passenger_info = pair.mapToPair(tuple->new Tuple2<>(tuple._1(),tuple._2()._1()));
            driver_info = pair.mapToPair(tuple->new Tuple2<>(tuple._1(),tuple._2()._2()));
        }
        //receive.savePassengerFile(jssc.sparkContext(),passenger_info);
        //receive.saveDriverFile(jssc.sparkContext(),driver_info);
        Function3<Integer, Optional<Map<String,Location>>, State<Map<String,Location>>,Map<String,Location>> update = (word, op, state)->{
            if(!state.exists()||op.isPresent()){
                state.update(op.get());
            }
            else{
                Map<String,Location> locs = op.get();
                Map<String,Location> up = new HashMap<>();
                for(String t : locs.keySet()){
                    if(!locs.get(t).getMatched()){
                        up.put(t, locs.get(t));
                    }
                }
                state.update(up);
            }
            return state.get();
        };
        passengers_each_area.mapWithState(StateSpec.function(function)).stateSnapshots().print();
        drivers_each_area.mapWithState(StateSpec.function(function)).stateSnapshots().print();
        jssc.checkpoint("checkpoint");
        //Dataset dataset = new _receive_info().readFile(jssc.sparkContext(), "passenger");
        //new SQLContext(SparkSession.builder().config(conf).getOrCreate());
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public JavaPairDStream<Integer,Map<String,Location>> mapToArea(JavaPairDStream<String,Location> pair){
        JavaPairDStream<Integer,Map<String,Location>> areas = pair.mapToPair(l->{
            Map<String,Location>loc = new HashMap<>();
            loc.put(l._1(), l._2());
            return new Tuple2<>(l._2()._getArea(false,l._1().equals("driver")),loc);
        });
        JavaPairDStream<Integer,Map<String,Location>> area =  areas.reduceByKey(((location, location2) -> {
            location.putAll(location2);
            return location;
        }));
        return area;
    }

    public JavaPairDStream<Integer,Map<String,Location>> mapToArea2(JavaPairDStream<String,Location> pair){
        JavaPairDStream<Integer,Map<String,Location>> areas = pair.mapToPair(l->{
            Map<String,Location>loc = new HashMap<>();
            loc.put(l._1(), l._2());
            return new Tuple2<>(l._2()._getArea(true,l._1().equals("driver")),loc);
        });
        JavaPairDStream<Integer,Map<String,Location>> area =  areas.reduceByKey(((location, location2) -> {
            location.putAll(location2);
            return location;
        }));
        return area;
    }


    public void savePassengerFile(JavaSparkContext sc,JavaDStream<Location> dstream){
        //sc.textFile("//data/passenger.txt").map(.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF();
        dstream.foreachRDD((rdd)->{
            if(!rdd.isEmpty()){
                SparkSession session = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
                Dataset<Row> df = session.createDataFrame(rdd, Map.class);
                df.show();
                df.write().mode(SaveMode.Append).save("passenger-info");
            }
        });
    }


    public void saveDriverFile(JavaSparkContext sc, JavaDStream<Location> dstream){
        //sc.textFile("//data/passenger.txt").map(.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF();
        dstream.foreachRDD((rdd)->{
            if(!rdd.isEmpty()){
                SparkSession session = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
                Dataset<Row> df = session.createDataFrame(rdd, Map.class);
                df.show();
                df.write().mode(SaveMode.Append).save("driver-info");
            }
        });
    }


    public JavaPairDStream<Integer, Tuple2<Map<String,Location>,Map<String,Location>>> match(JavaPairDStream<Integer,Map<String,Location>>passenger, JavaPairDStream<Integer,Map<String,Location>>driver){
        JavaPairDStream<Integer, Tuple2<Map<String,Location>,Map<String,Location>>> areaInfo = passenger.join(driver);
        return areaInfo.mapToPair(tuple->{
            Map<String,Location> passengers = tuple._2()._2();
            Map<String,Location> drivers = tuple._2()._2();
            Map<String,Location> m1 = new HashMap<>();
            Map<String,Location> m2 = new HashMap<>();
            for(String p : passengers.keySet()){
                Tuple2<String,String> match = null;
                double distance = Integer.MAX_VALUE;
                Location passenger_location = passengers.get(p);
                if(passenger_location.getMatched()){
                    continue;
                }
                for(String d : passengers.keySet()){
                    if(passenger_location.getMatched()){
                        continue;
                    }
                    Location driver_location = drivers.get(d);
                    double dis =  passenger_location._far_away_from(driver_location);
                    if(dis<distance){
                        match = new Tuple2<>(p,d);
                        distance = dis;
                    }
                }
                Location matched_passenger = passengers.get(p).birth();
                if(match!=null&&distance<10){
                    Location matched_driver = drivers.get(match._2()).birth();
                    matched_driver.setMatched(true);
                    m1.put(p,matched_passenger);
                    m2.put(match._2(), matched_driver);
                }
                else{
                    matched_passenger.setMatched(false);
                    m1.put(p, matched_passenger);
                }
            }
            if(m2.size()<drivers.size()){
                for(String d : drivers.keySet()){
                    if(!m2.containsKey(d)){
                        Location loc = drivers.get(d).birth();
                        loc.setMatched(false);
                        m2.put(d, loc);
                    }
                }
            }
            return new Tuple2<>(tuple._1(),new Tuple2<>(m1,m2));
        });
    }

    public Dataset readFile(JavaSparkContext sc, String type){
        SQLContext sqlContext = new SQLContext(sc);
        //Dataset dataset = sqlContext.jsonFile("info");
        Dataset dataset = sqlContext.load(type+"-info").toDF();
        //dataset.show();
        //Dataset<Row> dataset = sqlContext.read.format("parquet").load("路径");
        return  dataset;
    }

    public JavaPairDStream<Integer,Map<String,Location>> removeMatched(JavaPairDStream<Integer,Map<String,Location>> match){
        return match.mapToPair(tuple->{
            Map<String,Location> map=new HashMap<>();
            Map<String,Location> m = tuple._2();
            for(String key :m.keySet()){
                if(!m.get(key).getMatched()){
                    map.put(key, m.get(key));
                }
            }
            return new Tuple2<>(tuple._1(), map);
        });
    }

}

