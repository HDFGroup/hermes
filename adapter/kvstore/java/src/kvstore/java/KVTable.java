package kvstore.java;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import hermes.java.Hermes;
import hermes.java.Bucket;
import hermes.java.Blob;
import hermes.java.MdLockType;
import hermes.java.UniqueId;

public class KVTable {
  Bucket bkt_;

  /** Emplace Constructor */
  KVTable(String table_name) {
    Hermes hermes = Hermes.getInstance();
    bkt_ = hermes.getBucket(table_name);
  }

  /** Serialize a Map into a blob */
  private <T> Blob mapToBlob(Map<String, T> map) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(map);
      oos.close();
      byte[] bytes = baos.toByteArray();
      return new Blob(ByteBuffer.wrap(bytes));
    } catch (IOException e) {
      e.printStackTrace();
      return Blob.fromString("");
    }
  }

  /** Deserialize a Map from a blob */
  private <T> Map<String, T> blobToMap(Blob blob) {
    try {
      ObjectInputStream ois = new ObjectInputStream(
              new ByteArrayInputStream(blob.array()));
      Map<String, T> map = (Map<String, T>) ois.readObject();
      ois.close();
      blob.close();
      return map;
    } catch (IOException e) {
      e.printStackTrace();
      return new HashMap<String, T>();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return new HashMap<String, T>();
    }
  }

  /**
   * Create or insert a record into the table
   *
   * @param key the record key
   * @param val the values to update in the record
   * @return None
   * */
  public <T> void update(String key, Map<String, T> val) {
    UniqueId blob_id = bkt_.getBlobId(key);
    if (blob_id.isNull()) {
      bkt_.lockBlob(blob_id, MdLockType.kExternalWrite);
      Blob blob = mapToBlob(val);
      blob_id = bkt_.put(key, blob);
      bkt_.unlockBlob(blob_id, MdLockType.kExternalWrite);
      blob.close();
    } else {
      bkt_.lockBlob(blob_id, MdLockType.kExternalWrite);
      Blob orig_blob = bkt_.get(blob_id);
      Map<String, T> old_val = blobToMap(orig_blob);
      for (Map.Entry<String, T> entry : val.entrySet()) {
        old_val.put(entry.getKey(), entry.getValue());
      }
      Blob new_blob = mapToBlob(old_val);
      bkt_.put(key, new_blob);
      bkt_.unlockBlob(blob_id, MdLockType.kExternalWrite);
      new_blob.close();
    }
  }

  /**
   * Get a subset of fields from a record
   *
   * @param key the record key
   * @param field the field in the record to update
   * @return The blob containing only the field's data
   * */
  public <T> Map<String, T> read(String key, Set<String> field_set) {
    UniqueId blob_id = bkt_.getBlobId(key);
    bkt_.lockBlob(blob_id, MdLockType.kExternalRead);
    Blob orig_blob = bkt_.get(blob_id);
    Map<String, T> old_val = blobToMap(orig_blob);
    bkt_.unlockBlob(blob_id, MdLockType.kExternalRead);
    return old_val;
  }

  /** Delete a record */
  public void erase(String key) {
    UniqueId blob_id = bkt_.getBlobId(key);
    bkt_.destroyBlob(blob_id);
  }

  /** Destroy this table */
  public void destroy() {
    bkt_.destroy();
  }
};
