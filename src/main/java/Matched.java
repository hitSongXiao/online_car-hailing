public class Matched {
    private Location passenger;
    private Location driver;
    private Long arriveTime;

    public Matched(Location passenger,Location driver){
        this.passenger = passenger;
        this.driver = driver;
        this.arriveTime = passenger.getArriveTime();
    }

    public Location getPassenger(){
        return passenger;
    }

    public Location getDriver(){
        return driver;
    }

    public Long getArriveTime(){
        return arriveTime;
    }

    @Override
    public String toString(){
        return "[Passenger:"+passenger+"driver:"+driver+"the time of arrive:"+arriveTime+"]\n";
    }
}
