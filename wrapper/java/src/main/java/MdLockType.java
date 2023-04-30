package src.main.java;
public enum MdLockType {
    kInternalRead,    /**< Internal read lock used by Hermes */
    kInternalWrite,   /**< Internal write lock by Hermes */
    kExternalRead,    /**< External is used by programs */
    kExternalWrite,   /**< External is used by programs */
}
