import java.io.Serializable;

public class Location implements Serializable {
    private Long time;
    private Long arriveTime;
    private int row;
    private int col;
    private int id;
    private String type;
    private double longitude ;
    private double latitude ;
    private boolean matched = false;

    public Location(long arriveTime,String type, double longitude, double latitude, int id) {
        this.time = System.currentTimeMillis();
        this.arriveTime = arriveTime;
        this.type = type;
        this.id = id;
        this.longitude = longitude;
        this.latitude = latitude;
        setNum();
    }


    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }


    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setId(int id){
        this.id = id;
    }

    public void setType(String type){
        this.type = type;
    }

    public void setTime(long time){
        this.time = time;
    }

    public void setMatched(boolean matched){
        this.matched = matched;
    }

    public double getLongitude() {
        return longitude;
    }


    public double getLatitude() {
        return latitude;
    }

    public String getType() {
        return type;
    }

    public int getId() {
        return id;
    }

    public int _getArea(){
        return (row*360+col);
    }

    public boolean getMatched(){
        return matched;
    }

    public long getTime(){
        return time;
    }

    public long getArriveTime(){
        return arriveTime;
    }


    private void setNum(){
        col = (int)Math.ceil(longitude/0.1);
        row = (int)Math.ceil(latitude/0.1);
    }


    /**
     * 计算两个位置的距离，测试阶段,为了简化,直接根据经纬度计算距离，使用时，可以
     * 调用百度的API计算（下面函数即实现该功能）
     * @param driver
     * @return
     */
    public double _far_away_from(Location driver) {
        double EARTH_RADIUS = 6378137;//赤道半径(单位m)

        double radLat1 = rad(latitude);
        double radLat2 = rad(driver.getLatitude());

        double radLon1 = rad(longitude);
        double radLon2 = rad(driver.getLongitude());

        if (radLat1 < 0)
            radLat1 = Math.PI / 2 + Math.abs(radLat1);// south
        if (radLat1 > 0)
            radLat1 = Math.PI / 2 - Math.abs(radLat1);// north
        if (radLon1 < 0)
            radLon1 = Math.PI * 2 - Math.abs(radLon1);// west
        if (radLat2 < 0)
            radLat2 = Math.PI / 2 + Math.abs(radLat2);// south
        if (radLat2 > 0)
            radLat2 = Math.PI / 2 - Math.abs(radLat2);// north
        if (radLon2 < 0)
            radLon2 = Math.PI * 2 - Math.abs(radLon2);// west
        double x1 = EARTH_RADIUS * Math.cos(radLon1) * Math.sin(radLat1);
        double y1 = EARTH_RADIUS * Math.sin(radLon1) * Math.sin(radLat1);
        double z1 = EARTH_RADIUS * Math.cos(radLat1);

        double x2 = EARTH_RADIUS * Math.cos(radLon2) * Math.sin(radLat2);
        double y2 = EARTH_RADIUS * Math.sin(radLon2) * Math.sin(radLat2);
        double z2 = EARTH_RADIUS * Math.cos(radLat2);

        double d = Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2)+ (z1 - z2) * (z1 - z2));
        //余弦定理求夹角
        double theta = Math.acos((EARTH_RADIUS * EARTH_RADIUS + EARTH_RADIUS * EARTH_RADIUS - d * d) / (2 * EARTH_RADIUS * EARTH_RADIUS));
        double dist = theta * EARTH_RADIUS;
        return dist;
    }

    private static double rad(double d)
    {
        return d * Math.PI / 180.0;
    }

    public Location birth(){
        Location location = new Location(arriveTime,type, longitude, latitude, id);
        location.setTime(time);
        return location;
    }

    /*
     * 计算两个位置的距离，实际使用阶段,可以直接调用百度API计算距离，为方便使用具体实现及
     * 各个类封装在BaiduDistance.jar中。
     * @param driver    司机的位置
     * @return 距离
     * @throws IOException
     */
    /*
    public Distance far_away_from(location driver) throws IOException {
        LatLng from = new LatLng(),to=new LatLng();
        from.setLng(longitude);
        from.setLat(latitude);
        to.setLng(driver._get_longitude());
        to.setLat(driver._get_latitude());
        return new GetDistance().getDistance(from, to);
    }
    */

    @Override
    public String toString() {
        //return "type:"+type+" \tid:"+id+" \tlongitude:" + longitude + " \tlatitude:" + latitude;
        return "Location [type=：" + type + ", id=：" + id + ", longitude:" + longitude + ", latitude:"+latitude+", arrived time:"+arriveTime+", matched:"+matched+", begin at:"+time+"]\n";
    }


}
