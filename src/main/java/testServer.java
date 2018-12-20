import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * 测试是否能够成功发送与接受信息
 */
public class testServer{
    public static void main(String args[]){
        try {
            Socket socket = new Socket("127.0.0.1",8080);
            while(true) {
                socket.setSoTimeout(400);
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                new testServer().getBack(stream, socket);
                System.out.append(stream.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int getBack(ByteArrayOutputStream CloneResult, Socket client) {
        byte[] buffer = new byte[1024];
        int len = 0;
        int length = 0;
        try {
            InputStream input = client.getInputStream();
            while ((length = input.read(buffer)) != -1) {
                CloneResult.write(buffer, 0, length);
                len += length;
            }
        } catch (SocketTimeoutException e) {
            return len;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return len;
    }
}
