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

#include <hermes_shm/util/formatter.h>
#include <hermes_shm/util/config_parse.h>
#include <hermes_shm/util/auto_trace.h>
#include <hermes_shm/util/logging.h>
#include "basic_test.h"
#include "hermes_shm/util/singleton.h"
#include <unistd.h>

TEST_CASE("TestPathParser") {
  setenv("PATH_PARSER_TEST", "HOME", true);
  auto x = hshm::ConfigParse::ExpandPath("${PATH_PARSER_TEST}/hello");
  unsetenv("PATH_PARSER_TEST");
  auto y = hshm::ConfigParse::ExpandPath("${PATH_PARSER_TEST}/hello");
  REQUIRE(x == "HOME/hello");
  REQUIRE(y == "${PATH_PARSER_TEST}/hello");
}

TEST_CASE("TestTerminal") {
  std::cout << "\033[1m" << "Bold text" << "\033[0m" << std::endl;
  std::cout << "\033[4m" << "Underlined text" << "\033[0m" << std::endl;
  std::cout << "\033[31m" << "Red text" << "\033[0m" << std::endl;
  std::cout << "\033[32m" << "Green text" << "\033[0m" << std::endl;
  std::cout << "\033[33m" << "Yellow text" << "\033[0m" << std::endl;
  std::cout << "\033[34m" << "Blue text" << "\033[0m" << std::endl;
  std::cout << "\033[35m" << "Magenta text" << "\033[0m" << std::endl;
  std::cout << "\033[36m" << "Cyan text" << "\033[0m" << std::endl;
}

TEST_CASE("TestAutoTrace") {
  AUTO_TRACE(0)

  TIMER_START("Example")
  sleep(1);
  TIMER_END()
}

TEST_CASE("TestLogger") {
  HILOG(kInfo, "I'm more likely to be printed: {}", 0)
  HILOG(kDebug, "I'm not likely to be printed: {}", 10)

  HERMES_LOG->SetVerbosity(kInfo);
  HILOG(kInfo, "I'm more likely to be printed (2): {}", 0)
  HILOG(kDebug, "I won't be printed: {}", 10)

#ifdef TEST_ERRORS
  HELOG(kError, "I will NOT cause an EXIT!")
  HELOG(kFatal, "I will cause an EXIT!")
#endif
}

TEST_CASE("TestFormatter") {
  int rank = 0;
  int i = 0;

  PAGE_DIVIDE("Test with equivalent parameters") {
    std::string name = hshm::Formatter::format("bucket{}_{}",
                                               rank, i);
    REQUIRE(name == "bucket0_0");
  }

  PAGE_DIVIDE("Test with equivalent parameters at start") {
    std::string name = hshm::Formatter::format("{}bucket{}",
                                               rank, i);
    REQUIRE(name == "0bucket0");
  }

  PAGE_DIVIDE("Test with too many parameters") {
    std::string name = hshm::Formatter::format("bucket",
                                               rank, i);
    REQUIRE(name == "bucket");
  }

  PAGE_DIVIDE("Test with fewer parameters") {
    std::string name = hshm::Formatter::format("bucket{}_{}",
                                               rank);
    REQUIRE(name == "bucket{}_{}");
  }

  PAGE_DIVIDE("Test with parameters next to each other") {
    std::string name = hshm::Formatter::format("bucket{}{}",
                                               rank, i);
    REQUIRE(name == "bucket00");
  }
}

struct SimpleClass {
  int a_;

  SimpleClass() {
    a_ = 100;
  }
};

DEFINE_SINGLETON_CC(SimpleClass)
DEFINE_GLOBAL_SINGLETON_CC(SimpleClass)

TEST_CASE("Singleton") {
  SimpleClass *cls[4];

  cls[0] = hshm::Singleton<SimpleClass>::GetInstance();
  cls[1] = hshm::GlobalSingleton<SimpleClass>::GetInstance();
  cls[2] = hshm::EasySingleton<SimpleClass>::GetInstance();
  cls[3] = hshm::EasyGlobalSingleton<SimpleClass>::GetInstance();

  for (int i = 0; i < 4; ++i) {
    REQUIRE(cls[i]->a_ == 100);
  }
}
