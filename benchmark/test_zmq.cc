//
// Created by lukemartinlogan on 8/25/23.
//

#include <zmq.hpp>
#include "basic_test.h"
#include "hrun/api/hrun_client.h"
#include "hrun_admin/hrun_admin.h"
#include "small_message/small_message.h"
#include "hermes_shm/util/timer.h"
#include "hrun/work_orchestrator/affinity.h"

/** ZeroMQ allocate + free request */
TEST_CASE("TestZeromqAllocateFree") {
  zmq::context_t context(1);

  // Create a PUSH socket to push requests
  zmq::socket_t pushSocket(context, ZMQ_PUSH);

  // Bind the PUSH socket to a specific address
  pushSocket.bind("ipc:///tmp/shared_memory");

  // Create a PULL socket to receive requests
  zmq::socket_t pullSocket(context, ZMQ_PULL);

  // Connect the PULL socket to the same address
  pullSocket.connect("ipc:///tmp/shared_memory");

  hshm::Timer t;
  t.Resume();
  size_t ops = (1 << 20);
  for (size_t i = 0; i < ops; ++i) {
    zmq::message_t message(sizeof(hrun::Task));
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

/** Single-thread performance of zeromq */
TEST_CASE("TestZeromqAllocateEmplacePop") {
  zmq::context_t context(1);

  // Create a PUSH socket to push requests
  zmq::socket_t pushSocket(context, ZMQ_PUSH);

  // Bind the PUSH socket to a specific address
  pushSocket.bind("ipc:///tmp/shared_memory");

  // Create a PULL socket to receive requests
  zmq::socket_t pullSocket(context, ZMQ_PULL);

  // Connect the PULL socket to the same address
  pullSocket.connect("ipc:///tmp/shared_memory");

  hshm::Timer t;
  t.Resume();
  size_t ops = (1 << 20);
  for (size_t i = 0; i < ops; ++i) {
    // Send a request
    zmq::message_t message(sizeof(hrun::Task));
    pushSocket.send(message, zmq::send_flags::none);

    // Receive the request
    zmq::message_t receivedMessage;
    zmq::recv_result_t result = pullSocket.recv(receivedMessage);
    REQUIRE(receivedMessage.size() == sizeof(hrun::Task));
  }
  t.Pause();

  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}