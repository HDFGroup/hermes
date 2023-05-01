package hermes_kvstore.java;
import hermes_kvstore.java.KVTable;
import hermes.java.Hermes;

public class KVStore {
    public static void connect() {
        Hermes hermes = Hermes.getInstance();
        hermes.create();
    }

    public static KVTable getTable(String table_name) {
        return new KVTable(table_name);
    }
}
