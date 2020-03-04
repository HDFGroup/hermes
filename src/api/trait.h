#ifndef TRAIT_H_
#define TRAIT_H_

#include <memory>
#include <string>
#include <array>
#include <vector>
#include <tuple>

#include "glog/logging.h"

#include "hermes.h"

namespace hermes {

namespace api {

template <class... Args>
class TraitSchema {
 public:
  class Trait {
   private:
    Trait(Args... args) {
      attr_values_ = std::tuple<Args...>(args...);
    }
    
   public:
    std::tuple<Args...> attr_values_;
    /** internal HERMES object owned by trait */
    std::shared_ptr<HERMES> m_HERMES_;
    
    // TBD
    static const Trait kDefault;
    
    ~Trait() {
//      attributes_.clear();
    }
    
    /** update a trait property */
    Status EditTrait(const std::string &key,
                     const std::string &value,
                     Context &ctx);
    
    friend class TraitSchema;
  }; // class Trait
  
  static const size_t arg_length_ = std::tuple_size<std::tuple<Args...>>::value;
  std::array<std::string, arg_length_> key_names_;
  std::vector<std::string> reg_names_;
//  std::vector<* Trait> reg_traits_;
  
  template <std::size_t N>
  using type = typename std::tuple_element<N, std::tuple<Args...>>::type;
  
  TraitSchema(const std::string reg_name,
              const std::array<std::string,arg_length_>& names) {
    key_names_ = names;
    reg_names_.push_back(reg_name);
  };
  
  Trait& CreateInstance(Context &ctx, Args... arg);
}; // class TraitSchema

template <class... Args>
typename TraitSchema<Args...>::Trait& TraitSchema<Args...>
  ::CreateInstance(Context &ctx, Args... args) {
                               
  LOG(INFO) << "Create TraitSchema instance\n";
  
  TraitSchema<Args...>::Trait trt(args...);
    
  std::cout << "(" << std::get<0>(key_names_) << ", " << std::get<1>(key_names_)
            << ", " << std::get<2>(key_names_) << ")\n";
  std::cout << "(" << std::get<0>(trt.attr_values_) << ", " << std::get<1>(trt.attr_values_)
            << ", " << std::get<2>(trt.attr_values_) << ")\n";
                               
  return trt;
}
  
template <class... Args>
Status TraitSchema<Args...>::Trait::EditTrait(const std::string& key,
                                              const std::string& value,
                                              Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Editing Trait\n";
    
  return ret;
}

}  // api
}  // hermes

#endif  //  TRAIT_H_
