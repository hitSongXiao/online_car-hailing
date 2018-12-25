import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;

import java.util.*;

import static org.apache.spark.api.java.StorageLevels.MEMORY_AND_DISK_SER;

public class _receive_info {

    public static void main(String args[]){
        _receive_info receive = new _receive_info();
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("online_car-hailing");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaPairDStream<String,Location> info = lines.mapToPair(line->{
            String value[] = line.split("\\s+");
            double longitude = Double.valueOf(value[1].split(":")[1]);
            double latitude = Double.valueOf(value[2].split(":")[1]);
            String typeId[] = value[0].split(":");
            long arriveTime = Long.valueOf(value[3].split(":")[1]);
            Location loc = new Location(arriveTime,typeId[0],longitude, latitude,Integer.valueOf(typeId[1]));
            return new Tuple2<>(value[0],loc);
        });
        info.persist().persist(MEMORY_AND_DISK_SER);
        JavaPairDStream<String,Location>passengers = info.filter((tuple)->(tuple._1().split(":")[0].equals("passenger")));
        receive.savePassengerFile(jssc.sparkContext(),passengers);
        JavaPairDStream<String,Location>drivers = info.filter((tuple)->(tuple._1().split(":")[0].equals("driver")));
        receive.saveDriverFile(jssc.sparkContext(),drivers);
        JavaPairDStream<Area, Tuple2<Long,Map<String,Location>>> passengers_each_area = receive.mapToArea(passengers);
        JavaPairDStream<Area, Tuple2<Long,Map<String,Location>>> drivers_each_area = receive.mapToArea(drivers);
        Function3<Area, Optional<Tuple2<Long,Map<String,Location>>>, State<Tuple2<Long,Map<String,Location>>>,Map<String,Location>> function = (word, op, state)->{
            if(!state.exists()||!op.isPresent()){
                state.update(op.get());
            }
            else{
                Map<String,Location> loc = new HashMap<>();
                loc.putAll(state.get()._2());
                loc.putAll(op.get()._2());
                state.update(new Tuple2<>(System.currentTimeMillis(),loc));
            }
            return state.get()._2();
        };
        JavaPairDStream<Area, Tuple2<Long,Map<String,Location>>> passenger_info = passengers_each_area.mapWithState(StateSpec.function(function)).stateSnapshots();
        JavaPairDStream<Area, Tuple2<Long,Map<String,Location>>> driver_info = drivers_each_area.mapWithState(StateSpec.function(function)).stateSnapshots();
        Tuple3<JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>>,JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>>,JavaPairDStream<Area,Set<Matched>>> tuple3 = null;
        JavaPairDStream<Area,Set<Matched>> matched=null;
        for(int i = 0;i<9;i++){
            driver_info = receive.changeArea(driver_info);
            tuple3 = receive.match(passenger_info, driver_info);
            passenger_info = tuple3._1();
            driver_info = tuple3._2();
            if(matched==null){
                matched = tuple3._3();
            }else{
                matched = matched.union(tuple3._3());
            }
        }
        driver_info = driver_info.mapToPair(tuple->new Tuple2<>(new Area(tuple._1()._getRealArea()),tuple._2()));
        matched.print();
        receive.saveMatchedFile(jssc.sparkContext(),matched);
        JavaPairDStream<Area, Tuple2<Long,Map<String,Location>>> driverMatched = receive.recoverArea(driver_info);
        JavaPairDStream<Area, Tuple2<Long,Map<String,Location>>> passengerMatched = passenger_info;
        Function3<Area, Optional<Tuple2<Long,Map<String,Location>>>, State<Tuple2<Long,Map<String,Location>>>,Map<String,Location>> update = (word, op, state)->{
            if(!state.exists()){
                state.update(op.get());
            }
            else{
                Map<String,Location> locs = op.get()._2();
                Map<String,Location> up = new HashMap<>();
                for(String t : locs.keySet()){
                    if(!locs.get(t).getMatched()){
                        up.put(t, locs.get(t));
                    }
                }
                state.update(new Tuple2<>(System.currentTimeMillis(), up));
            }
            return state.get()._2();
        };
        passengerMatched.mapWithState(StateSpec.function(update)).stateSnapshots();
        driverMatched.mapWithState(StateSpec.function(update)).stateSnapshots();
        jssc.checkpoint("checkpoint");
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> changeArea(JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> pair){
        return pair.mapToPair(tuple->{
            Area next = tuple._1()._getNextArea();
            return new Tuple2<>(next, tuple._2());
        });
    }

    public JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> recoverArea(JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> pair){
        return pair.mapToPair(tuple->{
            Area realArea = new Area(tuple._1()._getRealArea());
            return new Tuple2<>(realArea, tuple._2());
        });
    }

    public JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> mapToArea(JavaPairDStream<String,Location> pair){
        JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> areas = pair.mapToPair(l->{
            Map<String,Location>loc = new HashMap<>();
            loc.put(l._1(), l._2());
            return new Tuple2<>(new Area(l._2()._getArea()),new Tuple2<>(System.currentTimeMillis(),loc));
        });
        JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> area =  areas.reduceByKey(((location, location2) -> {
            Map<String,Location> map = new HashMap<>();
            long time;
            if(location._1()>location2._1()){
                map.putAll(location2._2());
                map.putAll(location._2());
                time = location._1();
            }else{
                map.putAll(location._2());
                map.putAll(location2._2());
                time = location2._1();
            }
            return new Tuple2<>(time, map);
        }));
        return area;
    }


    public void savePassengerFile(JavaSparkContext sc, JavaPairDStream<String,Location> dstream){
        dstream.map(ds->ds._2()).foreachRDD(rdd->{
            if(!rdd.isEmpty()){
                SparkSession session = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
                Dataset<Row> df = session.createDataFrame(rdd.rdd(), Location.class);
                //df.show();
                df.write().mode(SaveMode.Append).save("passenger-info");
            }
        });
    }

    public void saveDriverFile(JavaSparkContext sc, JavaPairDStream<String,Location> dstream){
        dstream.map(ds->ds._2()).foreachRDD(rdd->{
            if(!rdd.isEmpty()){
                SparkSession session = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
                Dataset<Row> df = session.createDataFrame(rdd.rdd(), Location.class);
                //df.show();
                df.write().mode(SaveMode.Append).save("driver-info");
            }
        });
    }

    public void saveMatchedFile(JavaSparkContext sc, JavaPairDStream<Area,Set<Matched>> matched){
        matched.map(tuple->Arrays.asList(tuple._2().toArray())).foreachRDD(rdd->{
            if(rdd!=null&&rdd.count()>0) {
                List<Object> l = rdd.reduce((list1, list2) -> {
                    List<Object> list = new ArrayList<>();
                    if (!list1.isEmpty() && !(list2 == null))
                        list.addAll(list1);
                    if (!list2.isEmpty() && !(list2 == null))
                        list.addAll(list2);
                    return list;
                });
                RDD<Object> r = rdd.context().parallelize(JavaConverters.asScalaIteratorConverter(l.iterator()).asScala().toSeq(), 4, JavaSparkContext$.MODULE$.fakeClassTag());
                if(!l.isEmpty())
                    r.saveAsTextFile("match");
            }
        });
    }


    public Tuple3<JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>>,JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>>,JavaPairDStream<Area,Set<Matched>>> match(JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>>passenger, JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>>driver){
        JavaPairDStream<Area, Tuple2<Tuple2<Long,Map<String,Location>>,Tuple2<Long,Map<String,Location>>>> areaInfo = passenger.join(driver);
        JavaPairDStream<Area, Tuple3<Tuple2<Long,Map<String,Location>>,Tuple2<Long,Map<String,Location>>,Set<Matched>>> pair =  areaInfo.mapToPair(tuple->{
            Set<Matched> matcheds = new HashSet<>();
            Map<String,Location> passengers = tuple._2()._1()._2();
            Map<String,Location> drivers = tuple._2()._2()._2();
            Map<String,Location> m1 = new HashMap<>();
            Map<String,Location> m2 = new HashMap<>();
            ArrayList<Tuple2<String,Location>> list1 = new ArrayList<>();
            ArrayList<Tuple2<String,Location>> list2 = new ArrayList<>();
            for(String key: passengers.keySet()){
                list1.add(new Tuple2<>(key,passengers.get(key)));
            }
            for(String key: drivers.keySet()){
                list2.add(new Tuple2<>(key,drivers.get(key)));
            }
            Comparator comparator = new sortByTime();
            list1.sort(comparator);
            Long driverTime = tuple._2()._2()._1();
            for(Tuple2<String,Location> p : list1){
                Tuple2<Location,Location> match = null;
                double distance = Integer.MAX_VALUE;
                String driverInfo = "";
                Location passenger_location = p._2();
                if(passenger_location.getMatched()){
                    continue;
                }
                for(Tuple2<String,Location> d : list2){
                    Location driver_location = d._2();
                    if(driver_location.getMatched()){
                        continue;
                    }
                    double dis =  passenger_location._far_away_from(driver_location);
                    if(dis<distance){
                        driverInfo = d._1();
                        match = new Tuple2<>(p._2(),d._2());
                        distance = dis;
                    }
                }
                Location matched_passenger = p._2().birth();
                if(match!=null){//&&distance<10){
                    Location matched_driver = match._2().birth();
                    match._2().setMatched(true);
                    matched_passenger.setMatched(true);
                    matched_driver.setMatched(true);
                    m1.put(p._1(),matched_passenger);
                    m2.put(driverInfo, matched_driver);
                    matcheds.add(new Matched(matched_passenger.birth(), matched_driver.birth()));
                }
                else{
                    matched_passenger.setMatched(false);
                    m1.put(p._1(), matched_passenger);
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
            return new Tuple2<>(tuple._1(),new Tuple3<>(new Tuple2<>(System.currentTimeMillis(),m1),new Tuple2<>(System.currentTimeMillis(),m2),matcheds));
        });
        JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> passenger_info = pair.mapToPair(tuple->new Tuple2<>(tuple._1(),tuple._2()._1()));
        JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> driver_info = pair.mapToPair(tuple->new Tuple2<>(tuple._1(),tuple._2()._2()));
        JavaPairDStream<Area,Set<Matched>> matched = pair.mapToPair(tuple->new Tuple2<>(tuple._1(),tuple._2()._3()));
        JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> passengers = passenger.union(passenger_info);
        JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> drivers = driver.union(driver_info);
        Function2<Tuple2<Long,Map<String,Location>>,Tuple2<Long,Map<String,Location>>,Tuple2<Long,Map<String,Location>>> reduce = ((tuple1,tuple2)->{
            Map<String,Location> map1 = tuple1._2();
            Map<String,Location> map2 = tuple2._2();
            Long time;
            Map<String,Location> map;
            if(tuple1._1()>tuple2._1()){
                time = tuple1._1();
                map = map2;
                map.putAll(map1);
            }
            else{
                time = tuple2._1();
                map = map1;
                map.putAll(map2);
            }
            return new Tuple2<>(time, map);
        });
        JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> finalPassenger = passengers.reduceByKey(reduce);
        JavaPairDStream<Area,Tuple2<Long,Map<String,Location>>> finalDriver = drivers.reduceByKey(reduce);
        return new Tuple3<>(finalPassenger,finalDriver,matched);
    }
}
class sortByTime implements Comparator{

    @Override
    public int compare(Object o, Object t1) {
        long value1,value2;
        if(o instanceof Location && t1 instanceof  Location) {
            value1 = (((Location) o).getArriveTime());
            value2 = ((Location) o).getArriveTime();
            if (value1 < value2) {
                return -1;
            }
            if (value1 == value2) {
                return 0;
            } else
                return 1;
        }
            return 0;
    }
}

