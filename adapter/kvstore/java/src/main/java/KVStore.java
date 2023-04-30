package src.main.java;
import src.main.java.KVTable;

public class KVStore {
    public native void connect();

    public native KVTable getTable(String table_name);
}
