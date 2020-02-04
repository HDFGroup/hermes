#ifndef TRAIT_H_
#define TRAIT_H_

#include <string>

#include "hermes.h"

namespace hermes
{
  namespace api
  {
    class Trait
    {
    public:

      // TBD

      static const Trait kDefault;

      /** update a trait property */
      Status EditTrait(const std::string& key,
                       const std::string& value,
                       Context& ctx);

      /** acquire a bucket and link to this trait as a side-effect */
      Bucket Acquire(const std::string& name, Context& ctx);

      /** link a bucket to this trait */
      Status Link(const Bucket& bkt, Context& ctx);

      /** unlink a bucket from this trait */
      Status Unlink(const Bucket& bkt, Context& ctx);

    };
  }  // end namespace api
}  // end namespace hermes

#endif  //  TRAIT_H_
