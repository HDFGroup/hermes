package hermes.java;
import java.util.Vector;
import hermes.java.UniqueId;
import hermes.java.MdLockType;
import hermes.java.Blob;

public class Bucket {
    public UniqueId bkt_id_;

    /**====================================
     * BUCKET operatoins
     * ===================================*/

    /** Used to construct bucket in JNI */
    private Bucket(UniqueId bkt_id) {
        bkt_id_ = bkt_id;
    }

    /** Lock this bucket */
    public native void lock(int lock_type);
    public void lock(MdLockType lock_type) {
        lock(lock_type.ordinal());
    }

    /** Unlock this bucket */
    public native void unlock(int lock_type);
    public void unlock(MdLockType lock_type) {
        unlock(lock_type.ordinal());
    }

    /** Get all blob IDs contained in this bucket */
    public native Vector<UniqueId> getContainedBlobIds();

    /** Destroy this bucket */
    public native void destroy();

    /**====================================
     * BLOB operations
     * ===================================*/

    /** Get the ID of a blob from a bucket */
    public native UniqueId getBlobId(String name);

    /** Get a blob from the bucket */
    public native Blob get(UniqueId blob_id);

    /** Put a blob into the bucket */
    public native UniqueId put(String blob_name, Blob data);

    /** Lock a blob */
    public native void lockBlob(UniqueId blob_id, int lock_type);
    public void lockBlob(UniqueId blob_id, MdLockType lock_type) {
        unlockBlob(blob_id, lock_type.ordinal());
    }

    /** Unlock a blob */
    public native void unlockBlob(UniqueId blob_id, int lock_type);
    public void unlockBlob(UniqueId blob_id, MdLockType lock_type) {
        unlockBlob(blob_id, lock_type.ordinal());
    }

    /** Destroy a blob */
    public native void destroyBlob(UniqueId blob_id);

    static {
        System.loadLibrary("hermes_java_Bucket");
    }
}
