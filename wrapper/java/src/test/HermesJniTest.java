package src.test;

import src.main.java.Bucket;
import src.main.java.Hermes;
import src.main.java.UniqueId;

public class HermesJniTest {
    public static void main(String[] args) {
        Hermes hermes = Hermes.getInstance();
        hermes.create();
        Bucket bkt = hermes.getBucket("hello");
        String data = "this is my DATA!";
        UniqueId blob_id = bkt.Put("0", data);
        String my_data = bkt.Get(blob_id);
        bkt.Destroy();
        assert(my_data.equals(data));
    }
}
