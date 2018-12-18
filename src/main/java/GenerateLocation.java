import java.io.IOException;
import java.net.Socket;
import java.util.Random;

public class GenerateLocation extends Thread {

    private String type;
    private Socket socket;
    private long id ;
    private int per_second;


    @Override
    public void run() {
        Random random = new Random();
        double longitude, latitude;
        try {
            for (int i = 0; i < 50; i++) {
                long start = System.currentTimeMillis();
                StringBuffer buf = new StringBuffer();
                for (int j = 0; j < per_second; j++) {
                    longitude = random.nextDouble();
                    latitude = random.nextDouble();
                    buf.append(type).append(":").append(id++).
                            append(" longitude:").append(longitude).
                            append(" latitude:").append(latitude).append("\n");
                }
                socket.getOutputStream().write(buf.toString().getBytes());
                long end = System.currentTimeMillis();
                if (end - start < 1000) {
                    try {
                        Thread.sleep(1000 - (end - start));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    socket.close();
                    System.err.println("To many data of " + type + " need to generate");
                    return;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param type       产生的类型："passenger"|"driver"
     * @param id         初始的id号
     * @param per_second 每秒产生的数量
     */
    public GenerateLocation(String host, int port, String type, long id, int per_second) {
        this.type = type;
        this.id = id;
        this.per_second = per_second;
        try {
            socket = new Socket(host, port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
