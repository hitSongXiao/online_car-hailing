import com.junfeng.distance.model.LatLng;
import com.junfeng.distance.util.MeasureUtil;

import java.io.IOException;

public class GetDistance {
    private String apk = "DHq996XlE41OxsrOUGGG1XAzScgnqrRL";

    public Distance getDistance(LatLng from, LatLng to) throws IOException {
        return new Distance(MeasureUtil.getDistance(apk, from, to), from, to);
    }
}
