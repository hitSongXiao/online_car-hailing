import java.io.IOException;

public class location {
    private double longitude;
    private double latitude;

    public location(double longitude,double latitude){
        this.longitude = longitude;
        this.latitude = latitude;
    }


    public double _get_longitude(){
        return longitude;
    }


    public double _get_latitude(){
        return latitude;
    }

    /**
     * 计算两个位置的距离，测试阶段,为了简化直接算点的直线距离
     * @param driver
     * @return
     */
    public double _far_away_from(location driver){
        double diff_longitude = longitude-driver._get_longitude();
        double diff_latitude = latitude-driver._get_latitude();
        return Math.sqrt((diff_longitude*diff_longitude+
                                diff_latitude*diff_latitude));
    }


    /**
     * 计算两个位置的距离，实际使用阶段,可以直接调用百度API计算距离，为方便使用具体实现及
     * 各个类封装在BaiduDistance.jar中。
     * @param driver    司机的位置
     * @return          距离
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


}
