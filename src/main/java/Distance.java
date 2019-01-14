import com.junfeng.distance.model.LatLng;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Distance {
    private LatLng from;
    private LatLng to;
    private double distance;
    private double time;

    public Distance(String dis, LatLng from, LatLng to) {
        this.from = from;
        this.to = to;
        Pattern Dis = Pattern.compile("(([\\d .]+)(公里|米))");
        Pattern Time = Pattern.compile("(([\\d .]+)(分钟|小时))");
        Matcher mat1 = Dis.matcher(dis);
        Matcher mat2 = Time.matcher(dis);
        if (mat1.find()) {
            String unit = mat1.group(3);
            switch (unit) {
                case "公里":
                    distance = 1000 * Double.parseDouble(mat1.group(2));
                    break;
                case "米":
                    distance = Double.parseDouble(mat1.group(2));
                    break;
                default:
                    break;
            }
        }
        if (mat2.find()) {
            switch (mat2.group(3)) {
                case "小时":
                    time = 60 * Double.parseDouble(mat2.group(2));
                    break;
                case "分钟":
                    time = Double.parseDouble(mat2.group(2));
                    break;
                default:
                    break;
            }
        }
    }

    public Distance(float distance, LatLng from, LatLng to) {
        this.time = -1;
        this.distance = distance;
        this.from = from;
        this.to = to;
    }


    public double getDistance() {
        return distance;
    }

    public double getTime() {
        return time;

    }

    public LatLng getFrom() {
        return from;
    }

    public LatLng getTo() {
        return to;
    }


    @Override
    public String toString() {
        String str = "from:longitude-" + String.valueOf(from.getLng())
                + " latitude-" + String.valueOf(from.getLat())
                + " \tto:longitude-" + String.valueOf(to.getLng())
                + " latitude-" + String.valueOf(to.getLat())
                + " \tdistance:" + String.valueOf(distance);
        if (time > 0) {
            str += " \ttime:" + String.valueOf(time);
        }
        return str;
    }
}
