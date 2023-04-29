package src.test;

import src.main.java.Bucket;
import src.main.java.Hermes;
import src.main.java.UniqueId;

public class HermesJniTest {
    public static void main(String[] args) {
        Hermes hermes = Hermes.getInstance();
        hermes.create();
        Bucket bkt = hermes.getBucket("hello");
        Blob data = "this is my DATA!";
        UniqueId blob_id = bkt.put("0", data);
        Blob my_data = bkt.get(blob_id);
        bkt.destroy();
        assert(my_data.equals(data));
    }
}
