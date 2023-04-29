package src.main.java;
import java.nio.ByteBuffer;

public class Blob {
    public ByteBuffer data_;
    long data_ptr_;
    long size_;
    long alloc_;

    public Blob(ByteBuffer data, long data_ptr, long size, long alloc) {
        data_ = data;
        data_ptr_ = data_ptr;
        size_ = size;
        alloc_ = alloc;
    }

    public static native Blob fromString(String data);

    public native void close();

    public boolean equals(Blob other) {
        return data_.equals(other.data_);
    }

    static {
        System.loadLibrary("hermes_src_main_Blob");
    }
}
