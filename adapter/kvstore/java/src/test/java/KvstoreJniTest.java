import org.junit.Test;
import static org.junit.Assert.*;
import java.nio.*;
import java.util.*;
import hermes_kvstore.java.KVStore;
import hermes_kvstore.java.KVTable;

import java.lang.management.ManagementFactory;

public class KvstoreJniTest {
    @Test
    public void testKvPutGet() throws Exception {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println(pid);
        KVStore.connect();
        KVTable table = KVStore.getTable("hello");/**/

        HashMap<String, String> record = new HashMap<String, String>();
        try {
            record.put("f0", "abcde");
            record.put("f1", "12345");
            record.put("f2", "ABCDE");
            table.update("0", record);
            table.update("0", record);
            Map<String, String> read_record = table.read("0");
            for (Map.Entry<String, String> entry : read_record.entrySet()) {
                assertTrue(entry.getValue().equals(record.get(entry.getKey())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
