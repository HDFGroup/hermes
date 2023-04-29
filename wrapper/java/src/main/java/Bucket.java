package src.main.java;


public class Bucket {
    public UniqueId bkt_id_;

    public Bucket(UniqueId bkt_id) {
        bkt_id_ = bkt_id;
    }

    public native void destroy();

    public native Blob get(UniqueId blob_id);

    public native UniqueId put(String blob_name, Blob data);

    public native void destroyBlob(UniqueId blob_id);

    static {
        System.loadLibrary("hermes_src_main_Bucket");
    }
}
