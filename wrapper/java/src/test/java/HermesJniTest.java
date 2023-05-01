import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Vector;
import hermes.java.Blob;
import hermes.java.Bucket;
import hermes.java.Hermes;
import hermes.java.UniqueId;
import hermes.java.MdLockType;

import java.lang.management.ManagementFactory;

public class HermesJniTest {
    @Test
    public void testBucketPutGet() {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println(pid);
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
