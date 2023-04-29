import org.junit.Test;
import static org.junit.Assert.*;

import src.main.java.Blob;
import src.main.java.Bucket;
import src.main.java.Hermes;
import src.main.java.UniqueId;

import java.lang.management.ManagementFactory;

public class HermesJniTest {
    @Test
    public void testBucketPutGet() {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        Hermes hermes = Hermes.getInstance();
        hermes.create();
        Bucket bkt = hermes.getBucket("hello");
        Blob old_data = Blob.fromString("this is my DATA!");
        UniqueId blob_id = bkt.put("0", old_data);
        Blob new_data = bkt.get(blob_id);
        bkt.destroy();
        assertTrue(new_data.equals(old_data));
        new_data.close();
        old_data.close();
    }
}
