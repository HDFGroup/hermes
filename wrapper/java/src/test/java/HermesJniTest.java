import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Vector;
import src.main.java.Blob;
import src.main.java.Bucket;
import src.main.java.Hermes;
import src.main.java.UniqueId;
import src.main.java.MdLockType;

import java.lang.management.ManagementFactory;

public class HermesJniTest {
    @Test
    public void testBucketPutGet() {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        Hermes hermes = Hermes.getInstance();
        hermes.create();
        Bucket bkt = hermes.getBucket("hello");
        Blob old_data = Blob.fromString("this is my DATA!");

        bkt.lock(MdLockType.kExternalWrite);
        UniqueId blob_id = bkt.put("0", old_data);
        bkt.unlock(MdLockType.kExternalWrite);

        bkt.lockBlob(blob_id, MdLockType.kExternalRead);
        Blob new_data = bkt.get(blob_id);
        bkt.unlockBlob(blob_id, MdLockType.kExternalRead);

        Vector<UniqueId> blob_ids = bkt.getContainedBlobIds();
        assertEquals(blob_ids.size(), 1);
        assertTrue(blob_ids.get(0).equals(blob_id));

        bkt.destroy();
        assertTrue(new_data.equals(old_data));
        new_data.close();
        old_data.close();
    }
}
