import org.junit.Test;
import static org.junit.Assert.*;
import java.nio.*;
import java.util.*;
import kvstore.java.KVStore;
import kvstore.java.KVTable;

import java.lang.management.ManagementFactory;

public class KvstoreJniTest {
    @Test
    public void testKvPutGet() {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println(pid);
        KVStore.connect();
        KVTable table = KVStore.getTable("hello");/**/

        HashMap<String, String> record = new HashMap<String, String>();
        record.put("f0", "abcde");
        record.put("f1", "12345");
        record.put("f2", "ABCDE");
        table.update("0", record);
    }
}
