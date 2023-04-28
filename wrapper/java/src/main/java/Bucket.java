package src.main.java;

public class Bucket {
    public native void Destroy();

    public native String Get(UniqueId blob_id);

    public native UniqueId Put(String blob_name, String data);

    public native void DestroyBlob(UniqueId blob_id);

    static {
        System.loadLibrary("hermes_src_main_Bucket");
    }
}
