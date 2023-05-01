package hermes.java;

public class UniqueId {
    public long unique_;
    public int node_id_;

    UniqueId(long unique, int node_id) {
        unique_ = unique;
        node_id_ = node_id;
    }

    public boolean equals(UniqueId other) {
        return unique_ == other.unique_ && node_id_ == other.node_id_;
    }

    public boolean isNull() { return unique_ == 0; }
}
