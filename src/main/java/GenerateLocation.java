import java.net.Socket;
import java.util.Random;

public class GenerateLocation {

    private String host = "127.0.0.1";
    private int port = 8080;
    private Socket socket = new Socket();


    public void _generate_passenger(int per_second) {
        try {
            _generate_locations(per_second);
        } catch (ToManyDataException e) {
            System.err.println("To many data of passenger need to generate");
            return;
        }
    }

    public void _generate_driver(int per_second) {
        try {
            _generate_locations(per_second);
        } catch (ToManyDataException e) {
            System.err.println("To many data of driver need to generate");
            return;
        }
    }

    public void _generate_locations(int perSecond) throws ToManyDataException {
        Random random = new Random();
        double longitude, latitude;
        for (int i = 0; i < 50; i++) {
            long start = System.currentTimeMillis();
            StringBuffer buf = new StringBuffer();
            for (int j = 0; j < perSecond; j++) {
                longitude = random.nextDouble();
                latitude = random.nextDouble();
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
