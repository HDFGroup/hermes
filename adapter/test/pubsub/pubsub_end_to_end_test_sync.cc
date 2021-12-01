#include <hermes/adapter/pubsub.h>
int main(int argc, char **argv) {
  hermes::pubsub::mpiInit(argc, argv);
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  int comm_size;
  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }
  else{
    config_file = getenv(kHermesConf);
  }

  auto connect_ret = hermes::pubsub::connect(config_file);

  if(connect_ret.Succeeded()) {
    auto attach_ret = hermes::pubsub::attach("test");
    assert(attach_ret.Succeeded());

    uint data_size = 6;
    std::vector<unsigned char*> full_data;
    unsigned char data1[6] = "test0";
    full_data.push_back(data1);
    unsigned char data2[6] = "test1";
    full_data.push_back(data2);
    unsigned char data3[6] = "test2";
    full_data.push_back(data3);

    for(unsigned char* data : full_data){
      auto publish_ret = hermes::pubsub::publish(
          "test", std::vector<unsigned char>(data, data + data_size));
      assert(publish_ret.Succeeded());
    }

    unsigned long num_messages = full_data.size() * comm_size;
    std::pair<std::vector<unsigned char>, hapi::Status> subscribe_ret;
    for(unsigned long i = 0; i < num_messages; i++) {
      subscribe_ret = hermes::pubsub::subscribe("test");
      assert(subscribe_ret.second.Succeeded());
    }

    auto detach_ret = hermes::pubsub::detach("test");
    assert(detach_ret.Succeeded());
  }
  auto disconnect_ret = hermes::pubsub::disconnect();
  MPI_Finalize();
}
