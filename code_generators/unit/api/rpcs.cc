/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

int main() {
  RPC_AUTOGEN_START
auto remote_f2 = 
  [mdm](const request &req, ) {
    auto result = mdm->Localf2();
    req.respond(result);
  };
server_engine_->define("f2", remote_f2);
auto remote_f3 = 
  [mdm](const request &req, int a, int b) {
    auto result = mdm->Localf3(a, b);
    req.respond(result);
  };
server_engine_->define("f3", remote_f3);
auto remote_f4 = 
  [mdm](const request &req, int a, std::vector<int> b,  Ctx) {
    auto result = mdm->Localf4(a, b);
    req.respond(result);
  };
server_engine_->define("f4", remote_f4);
auto remote_f5 = 
  [mdm](const request &req, int a, std::vector<int> b,  Ctx) {
    auto result = mdm->Localf5(a, b);
    req.respond(result);
  };
server_engine_->define("f5", remote_f5);
auto remote_f6 = 
  [mdm](const request &req, int a, std::vector<int> b,  Ctx) {
    auto result = mdm->Localf6(a, b);
    req.respond(result);
  };
server_engine_->define("f6", remote_f6);
auto remote_f99 = 
  [mdm](const request &req, ) {
    auto result = mdm->Localf99();
    req.respond(result);
  };
server_engine_->define("f99", remote_f99);
auto remote_f102 = 
  [mdm](const request &req, ) {
    auto result = mdm->Localf102();
    req.respond(result);
  };
server_engine_->define("f102", remote_f102);
  auto remote_f100 = 
    [mdm](const request &req, ) {
      auto result = mdm->Localf100();
      req.respond(result);
    };
  server_engine_->define("f100", remote_f100);
    auto remote_f101 = 
      [mdm](const request &req, ) {
        auto result = mdm->Localf101();
        req.respond(result);
      };
    server_engine_->define("f101", remote_f101);
