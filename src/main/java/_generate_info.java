import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class _generate_info {
    public static void main(String args[]) {
        try {
            ServerSocket server = new ServerSocket(9999);
            while(true){
                Socket client = server.accept();
                System.out.println("服务器已连接");
                GenerateLocation generate_passenger = new GenerateLocation(client, "passenger", 1, 5);
                GenerateLocation generate_driver = new GenerateLocation(client, "driver", 1, 5);
                generate_driver.start();
                generate_passenger.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
