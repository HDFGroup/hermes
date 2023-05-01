package hermes.java;
import java.nio.ByteBuffer;

public class Blob {
    public ByteBuffer data_;
    long data_ptr_;
    long size_;
    long alloc_;
    boolean is_native_;

    /** Create a blob from JNI */
    private Blob(ByteBuffer data, long data_ptr, long size, long alloc) {
        data_ = data;
        data_ptr_ = data_ptr;
        size_ = size;
        alloc_ = alloc;
        is_native_ = true;
    }

    /** Createa blob within Java */
    public Blob(ByteBuffer data) {
        data_ = data;
        size_ = data.capacity();
        is_native_ = false;
    }

    /** Convert a string to a Blob */
    public static native Blob fromString(String data);

    /** Call ByteBuffer array method */
    public byte[] array() {
        return data_.array();
    }

    /** Free data allocated by JNI */
    private native void free_native();

    /** Release this blob */
    public void close() {
        if (is_native_) {
            free_native();
        }
    }

    public boolean equals(Blob other) {
        return data_.equals(other.data_);
    }

    static {
        System.loadLibrary("hermes_src_main_Blob");
    }
}
