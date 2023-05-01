package kvstore.java;
import kvstore.java.KVTable;
import hermes.java.Hermes;

public class KVStore {
    public static void connect() {
        Hermes hermes = Hermes.getInstance();
        hermes.create();
    }

    public KVTable getTable(String table_name) {
        return new KVTable(table_name);
    }
}
