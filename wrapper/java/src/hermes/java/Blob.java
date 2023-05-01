package hermes.java;
import java.nio.ByteBuffer;

public class Blob {
    public ByteBuffer data_;
    long size_;
    long alloc_;
    boolean is_native_;

    /** Create a blob from JNI */
    private Blob(ByteBuffer data, long size, long alloc) {
        data_ = data;
        size_ = size;
        alloc_ = alloc;
        is_native_ = true;
        data_.position(0);
    }

    /** Create a blob within Java */
    public Blob(ByteBuffer data) {
        data_ = ByteBuffer.allocateDirect(data.capacity());
        data_.put(data);
        size_ = data.capacity();
        alloc_ = 0;
        is_native_ = false;
        data_.position(0);
    }

    /** Convert a string to a Blob */
    public static Blob fromString(String data) {
        return new Blob(ByteBuffer.wrap(data.getBytes()));
    }

    /** Call ByteBuffer array method */
    public byte[] array() {
        return data_.array();
    }

    /** Free data allocated by JNI */
    private native void freeNative();

    /** Release this blob */
    public void close() {
        if (is_native_) {
            freeNative();
        }
    }

    public boolean equals(Blob other) {
        return data_.equals(other.data_);
    }

    static {
        System.loadLibrary("hermes_java_Blob");
    }
}
