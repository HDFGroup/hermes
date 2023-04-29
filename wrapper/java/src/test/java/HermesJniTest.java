package src.test;
import org.junit.Test;
import static org.junit.Assert.*;

import src.main.java.Blob;
import src.main.java.Bucket;
import src.main.java.Hermes;
import src.main.java.UniqueId;

public class HermesJniTest {
    @Test
    public void testBucketPutGet() {
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
