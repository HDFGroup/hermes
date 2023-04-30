package src.main.java;
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
  public native void update(const std::string &key, KVRecord &val);

  /**
   * Get a subset of fields from a record
   *
   * @param key the record key
   * @param field the field in the record to update
   * @return The blob containing only the field's data
   * */
  Map<String, ByteBuffer> Read(const std::string &key, const KVFieldSet &field);

  /** Delete a record */
  void Erase(const std::string &key);

  /** Destroy this table */
  void Destroy();
};
