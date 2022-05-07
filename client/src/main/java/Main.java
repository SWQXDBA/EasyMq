public class Main {

    public static void main(String[] args) {
        EasyClient client = new EasyClient(8080,"localhost");
        client.run();
    }
}
