import java.io.Serializable;

public class Matched implements Serializable {
    private int passenger;
    private int driver;
    private Long arriveTime;
    private double longitude;
    private double latitude;
    private Long costTime;

    public Matched(Location passenger,Location driver){
        this.passenger = passenger.getId();
        this.driver = driver.getId();
        this.arriveTime = passenger.getArriveTime();
        this.latitude = passenger.getLatitude();
        this.longitude = passenger.getLongitude();
        this.costTime = (System.currentTimeMillis()-passenger.getTime())/1000;
    }

    public int getPassenger(){
        return passenger;
    }

    public int getDriver(){
        return driver;
    }

    public Long getArriveTime(){
        return arriveTime;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public Long getCostTime() {
        return costTime;
    }

    @Override
    public String toString(){
        return "[Passenger:"+passenger+", driver:"+driver+", the time of arrive:"+arriveTime+", cost time:"+costTime+"]\n";
    }
}
