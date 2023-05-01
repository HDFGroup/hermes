package hermes_kvstore.java;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
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
  private <T> Blob blobSerialize(T obj) throws IOException {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();
      byte[] bytes = baos.toByteArray();
      return new Blob(ByteBuffer.wrap(bytes));
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  /** Deserialize a Map from a blob */
  private <T> T blobDeserialize(Blob blob) throws IOException, ClassNotFoundException {
    try {
      byte[] bytes = blob.array();
      ByteArrayInputStream istream = new ByteArrayInputStream(bytes);
      ObjectInputStream ois = new ObjectInputStream(istream);
      T obj = (T) ois.readObject();
      ois.close();
      blob.close();
      return obj;
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw e;
    }
  }

  public String getFieldKey(String key, String field_name) {
    return String.format("%s-%s", key, field_name);
  }

  public String getKvMetadataKey() {
    return "hermes-kvmetadata";
  }

  /**
   * Insert a new record into the table
   * */
  public <T> void insert(String key, Map<String, T> val) throws IOException {
    /*UniqueId md_id = bkt_.getBlobId(getKvMetadataKey());
    if (md_id.isNull()) {
      Set<String> keys = new HashSet<String>(val.keySet());
      Blob md_blob = blobSerialize(keys);
      bkt_.put(getKvMetadataKey(), md_blob);
      md_blob.close();
    }
    for (Map.Entry<String, T> entry : val.entrySet()) {
      String field_key = getFieldKey(key, entry.getKey());
      Blob blob = blobSerialize(entry.getValue());
      bkt_.put(field_key, blob);
      blob.close();
    }*/
    Blob blob = blobSerialize(val);
    bkt_.put(key, blob);
    blob.close();
  }

  /**
   * Create or insert a record into the table
   *
   * @param key the record key
   * @param val the values to update in the record
   * @return None
   * */
  public <T> void update(String key, Map<String, T> val) throws IOException, ClassNotFoundException {
    /*insert(key, val);*/
    UniqueId blob_id = bkt_.getBlobId(key);
    if (blob_id.isNull()) {
      insert(key, val);
    } else {
      bkt_.lockBlob(blob_id, MdLockType.kExternalWrite);
      Blob orig_blob = bkt_.get(blob_id);
      Map<String, T> old_val = blobDeserialize(orig_blob);
      old_val.putAll(val);
      Blob new_blob = blobSerialize(old_val);
      bkt_.put(key, new_blob);
      bkt_.unlockBlob(blob_id, MdLockType.kExternalWrite);
      new_blob.close();
    }
  }

  /**
   * Get a subset of fields from a record
   *
   * @param key the record key
   * @param field_set the field in the record to update
   * @return The blob containing only the field's data
   * */
  public <T> Map<String, T> read(String key, Set<String> field_set) throws IOException, ClassNotFoundException {
    /*HashMap<String, T> map = new HashMap<String, T>();
    if (field_set.isEmpty()) {
      UniqueId md_id = bkt_.getBlobId(getKvMetadataKey());
      Blob md_blob = bkt_.get(md_id);
      field_set = (HashSet<String>)blobDeserialize(md_blob);
    }
    for (String field_name : field_set) {
      UniqueId blob_id = bkt_.getBlobId(getFieldKey(key, field_name));
      Blob blob = bkt_.get(blob_id);
      map.put(field_name, blobDeserialize(blob));
    }
    return map;*/

    UniqueId blob_id = bkt_.getBlobId(key);
    bkt_.lockBlob(blob_id, MdLockType.kExternalRead);
    Blob orig_blob = bkt_.get(blob_id);
    Map<String, T> old_val = blobDeserialize(orig_blob);
    bkt_.unlockBlob(blob_id, MdLockType.kExternalRead);
    return old_val;
  }

  public <T> Map<String, T> read(String key) throws IOException, ClassNotFoundException {
    return read(key, new HashSet<String>());
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
