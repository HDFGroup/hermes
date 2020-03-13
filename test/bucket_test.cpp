#include <iostream>
#include <unordered_map>

#include "zlib.h"

#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"

struct MyTrait {
	int compress_level;
	// optional function pointer if only known at runtime
};

void add_buffer_to_vector(hermes::api::Blob &vector, const char *buffer, uLongf length) {
    for (int character_index = 0; character_index < length; character_index++) {
        char current_character = buffer[character_index];
        vector.push_back(current_character);
    }
}

// The Trait implementer must define callbacks that match the VBucket::TraitFunc type.
int compress_blob(hermes::api::Blob &blob, void *trait) {
	MyTrait *my_trait = (MyTrait *)trait;

	// If Hermes is already linked with a compression library, you can call the function directly here.
	// If not, the symbol will have to be dynamically loaded and probably stored as a pointer in the Trait.
	uLongf source_length = blob.size();
	uLongf destination_length = compressBound(source_length);
	char *destination_data = new char [destination_length];
	 
	LOG(INFO) << "Compressing blob\n";
	 
	if (destination_data == nullptr)
		return Z_MEM_ERROR;

	int return_value = compress2((Bytef *) destination_data,
															&destination_length, (Bytef *) blob.data(), source_length,
															my_trait->compress_level);
	hermes::api::Blob destination {0};
	//TODO: where to store compressed data
	add_buffer_to_vector(destination, destination_data, destination_length);
	delete [] destination_data;
}


int main()
{
	std::shared_ptr<hermes::api::HERMES> hermes_app = std::make_shared<hermes::api::HERMES>();
  hermes::api::Context ctx;
  
  hermes::api::Bucket my_bucket("compression", hermes_app);
	hermes_app->Display_bucket();
  hermes::api::Blob p1 (255, 1024);
	hermes::api::Blob p2 (p1);
  my_bucket.Put("Blob1", p1, ctx);
  my_bucket.Put("Blob2", p2, ctx);
	if (my_bucket.Contain_blob("Blob1") == 1)
		std::cout<< "Found Blob1\n";
	else
		std::cout<< "Not found Blob1\n";
	if (my_bucket.Contain_blob("Blob2") == 1)
		std::cout<< "Found Blob2\n";
	else
		std::cout<< "Not found Blob2\n";

  hermes::api::VBucket my_vb("VB1", hermes_app);
	hermes_app->Display_vbucket();
  my_vb.Link("Blob1", "compression", ctx);
  my_vb.Link("Blob2", "compression", ctx);
	if(my_vb.Contain_blob("Blob1", "compression") == 1)
		std::cout << "Found Blob1 from compression bucket in VBucket VB1\n";
	else
		std::cout << "Not found Blob1 from compression bucket in VBucket VB1\n";
	if(my_vb.Contain_blob("Blob2", "compression") == 1)
		std::cout << "Found Blob2 from compression bucket in VBucket VB1\n";
	else
		std::cout << "Not found Blob2 from compression bucket in VBucket VB1\n";
	
	// compression level
  struct MyTrait trait {6};
  my_vb.Attach(&trait, compress_blob, ctx);   // compress action to data starts

///////  
  my_vb.Unlink("Blob1", "VB1", ctx);
  my_vb.Detach(&trait, ctx);
 
  return 0;
}
