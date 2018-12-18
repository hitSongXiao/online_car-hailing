public class _generate_info {
    public static void main(String args[]) {
        GenerateLocation generate_passenger = new GenerateLocation("127.0.0.1", 8080, "passenger", 1, 80);
        GenerateLocation generate_driver = new GenerateLocation("127.0.0.1", 8080, "driver", 1, 80);
        generate_driver.start();
        generate_passenger.start();

    }
}
