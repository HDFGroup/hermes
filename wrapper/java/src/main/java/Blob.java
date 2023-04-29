package src.main.java;
import java.nio.ByteBuffer;

public class Blob {
    public ByteBuffer data_;
    long data_ptr_;
    long alloc_;

    public Blob(ByteBuffer data, long data_ptr, long alloc) {
        data_ = data;
        data_ptr_ = data_ptr;
        alloc_ = alloc;
    }

    protected native void close();

    static {
        System.loadLibrary("hermes_src_main_Blob");
    }
}
