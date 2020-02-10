#ifndef BUCKET_H_
#define BUCKET_H_

#include <memory>
#include <string>

#include "hermes.h"

namespace hermes
{
namespace api
{
    class Bucket
    {
    public:
        std::string name;
      
        /** internal HERMES object owned by Bucket */
        std::shared_ptr<HERMES> m_HERMES;
        
        // TODO: Think about the Big Three
        
        Bucket ();
        
        Bucket (std::string initial_name);

        /** rename this bucket */
        Status Rename(const std::string& new_name,
                  Context& ctx);

        /** release this bucket and free its associated resources */
        Status Release(Context& ctx);

        /** put a blob on this bucket */
        Status Put(const std::string& name, const Blob& data, Context& ctx);

        /** get a blob on this bucket */
        const Blob& Get(const std::string& name, Context& ctx);

        /** delete a blob from this bucket */
        Status DeleteBlob(const std::string& name, Context& ctx);

        /** rename a blob on this bucket */
        Status RenameBlob(const std::string& old_name,
                      const std::string& new_name,
                      Context& ctx);
        
    private:
        std::vector<Blob> blobs;
    };
}  // api
}  // hermes

#endif  // BUCKET_H_
