package src.main.java;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import hermes.src.main.java.Bucket;

public class KVTable {
  Bucket bkt_;

  /** Emplace Constructor */
  KVTable(Bucket bkt) {
    bkt_ = bkt;
  }

  /**
   * Create or insert a record into the table
   *
   * @param key the record key
   * @param val the values to update in the record
   * @return None
   * */
  public native void update(String key, Map<String, ByteBuffer> val);

  /**
   * Get a subset of fields from a record
   *
   * @param key the record key
   * @param field the field in the record to update
   * @return The blob containing only the field's data
   * */
  public native Map<String, ByteBuffer> read(String key, Set<String> field_set);

  /** Delete a record */
  public native void erase(String key);

  /** Destroy this table */
  public native void destroy();
};
