#ifndef ID_H_
#define ID_H_

// See https://www.ilikebigbits.com/2014_05_06_type_safe_handles.html

namespace hermes {

namespace api {

template<class Tag, class T, T default_value>
  
class ID {
 private:
  T m_val_;
    
 public:
  static ID Invalid() { return ID(); }

  // Defaults to ID::invalid()
  ID() : m_val_(default_value) { }

  // Explicit constructor:
  explicit ID(T val) : m_val_(val) { }

  // Explicit conversion to get back the T:
  explicit operator T() const { return m_val_; }

  friend bool operator==(ID a, ID b) { return a.m_val == b.m_val; }
  friend bool operator!=(ID a, ID b) { return a.m_val != b.m_val; }
}; // class ID

}  // api namespace
}  // hermes namespace

#endif  // ID_H_
