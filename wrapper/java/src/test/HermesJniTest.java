package src.test;

public class HermesJniTest {
    public static void main(String[] args) {
        Hermes hermes = Hermes.getInstance();
        hermes.create();
        Bucket bkt = hermes.getBucket("hello");
        Blob data = Blob.fromString("this is my DATA!");
        UniqueId blob_id = bkt.put("0", data);
        Blob my_data = bkt.get(blob_id);
        bkt.destroy();
        assert(my_data.equals(data));
    }
}
