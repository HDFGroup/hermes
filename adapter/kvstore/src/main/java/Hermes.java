package src.main.java;
import src.main.java.Bucket;

public class Hermes {
    private static Hermes instance_ = null;

    static public Hermes getInstance() {
        if (instance_ == null) {
            instance_ = new Hermes();
        }
        return instance_;
    }

    public native void create();
    public native Bucket getBucket(String bkt_name);

    static {
        System.loadLibrary("hermes_src_main_Hermes");
    }
}
