import java.io.IOException;
import java.net.Socket;
import java.util.Random;

public class GenerateLocation {

    private String host = "127.0.0.1";
    private int port = 8080;
    private Socket socket = new Socket();
    private long passengerId = 0;
    private long driverId = 0;


    public void _generate_passenger(int per_second) {
        String type = "passenger";
        try {
            _generate_locations(type,passengerId, per_second);
        } catch (ToManyDataException e) {
            System.err.println("To many data of passenger need to generate");
            return;
        }
    }

    public void _generate_driver(int per_second) {
        String type = "driver";
        try {
            _generate_locations(type,driverId, per_second);
        } catch (ToManyDataException e) {
            System.err.println("To many data of driver need to generate");
            return;
        }
    }

    public void _generate_locations(String type, long id, int perSecond) throws ToManyDataException {
        Random random = new Random();
        double longitude, latitude;
        for (int i = 0; i < 50; i++) {
            long start = System.currentTimeMillis();
            StringBuffer buf = new StringBuffer();
            for (int j = 0; j < perSecond; j++) {
                longitude = random.nextDouble();
                latitude = random.nextDouble();
                buf.append(type).append(":").append(++id).
                        append(" longitude:").append(longitude).
                        append(" latitude:").append(latitude).append("\n");
            }
            try {
                socket.getOutputStream().write(buf.toString().getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            if (end - start < 1000) {
                try {
                    Thread.sleep(1000 - (end - start));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                throw new ToManyDataException();
            }
        }
    }
}
