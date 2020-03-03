#ifndef TRAIT_H_
#define TRAIT_H_

#include <memory>
#include <string>
#include <array>
#include <unordered_map>

#include "glog/logging.h"

#include "hermes.h"

namespace hermes {

namespace api {
  
class Trait {
 private:
  std::string name_;
  std::unordered_map<std::string, std::string> attributes_;
      
 public:
  /** internal HERMES object owned by trait */
  std::shared_ptr<HERMES> m_HERMES_;
  
  // TBD
  static const Trait kDefault;
        
  Trait() : name_("default_trait") {
    //TODO: initialize kDefault
    LOG(INFO) << "Create default Trait " << std::endl;
  };
        
  Trait(std::string initial_name) : name_(initial_name) {
    //TODO: initialize kDefault
    LOG(INFO) << "Create Trait " << initial_name << std::endl;
  };
  
  ~Trait() {
    name_.clear();
    attributes_.clear();
  }
      
  /** get the name of trait */
  std::string GetName() const {
    return this->name_;
  }
  
  /** update a trait property */
  Status EditTrait(const std::string &key,
                   const std::string &value,
                   Context &ctx);
}; // class Trait

template <class... Args>
class TraitSchema {
 public:
  static const size_t L = std::tuple_size<std::tuple<Args...>>::value;
  std::array<std::string,L> key_name;
    
  TraitSchema(const std::array<std::string,L>& names) {
    key_name = names;
  };
    
  template <std::size_t N>
  using type = typename std::tuple_element<N, std::tuple<Args...>>::type;
    
  Trait CreateInstance(Context &ctx);
};
#include "trait.inl"

}  // api
}  // hermes

#endif  //  TRAIT_H_
