//
// Created by llogan on 7/1/23.
//

#include "basic_test.h"
#include <boost/context/fiber_fcontext.hpp>
#include "hermes_shm/util/timer.h"
#include "hermes_shm/util/logging.h"
#include <boost/coroutine2/all.hpp>


void function() {
}

TEST_CASE("TestBoostFiber") {
  hshm::Timer t;
  t.Resume();
  size_t ops = (1 << 20);

  for (size_t i = 0; i < ops; ++i) {
    int a;
    boost::context::fiber source([&a](boost::context::fiber &&sink) {
      function();
      return std::move(sink);
    });
  }

  t.Pause();
  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

template< std::size_t Max, std::size_t Default, std::size_t Min >
class simple_stack_allocator
{
 public:
  static std::size_t maximum_stacksize()
  { return Max; }

  static std::size_t default_stacksize()
  { return Default; }

  static std::size_t minimum_stacksize()
  { return Min; }

  void * allocate( std::size_t size) const
  {
    BOOST_ASSERT( minimum_stacksize() <= size);
    BOOST_ASSERT( maximum_stacksize() >= size);

    void * limit = malloc( size);
    if ( ! limit) throw std::bad_alloc();

    return static_cast< char * >( limit) + size;
  }

  void deallocate( void * vp, std::size_t size) const
  {
    BOOST_ASSERT( vp);
    BOOST_ASSERT( minimum_stacksize() <= size);
    BOOST_ASSERT( maximum_stacksize() >= size);

    void * limit = static_cast< char * >( vp) - size;
    free( limit);
  }
};

typedef simple_stack_allocator<
    8 * 1024 * 1024,
    64 * 1024,
    32> stack_allocator;

int value1;
namespace bctx = boost::context::detail;

void f3( bctx::transfer_t t_) {
  ++value1;
  bctx::transfer_t t = bctx::jump_fcontext( t_.fctx, 0);
  ++value1;
  bctx::jump_fcontext( t.fctx, t.data);
}


TEST_CASE("TestBoostFcontext") {
  value1 = 0;
  stack_allocator alloc;
  int size = 128;

  hshm::Timer t;
  t.Resume();
  size_t ops = (1 << 20);

  void *sp = alloc.allocate(size);
  for (size_t i = 0; i < ops; ++i) {
    bctx::fcontext_t ctx = bctx::make_fcontext(sp, size, f3);
    bctx::transfer_t t = bctx::jump_fcontext(ctx, 0);
    bctx::jump_fcontext(t.fctx, 0);
  }
  alloc.deallocate(sp, size);

  t.Pause();
  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}

using namespace boost::coroutines2;

void myCoroutine(coroutine<void>::push_type& yield) {
  for (int i = 1; i <= 5; ++i) {
    yield();
  }
}

TEST_CASE("TestBoostCoroutine") {
  hshm::Timer t;
  t.Resume();
  size_t ops = (1 << 20);

  for (size_t i = 0; i < ops; ++i) {
    coroutine<void>::pull_type myCoroutineInstance(myCoroutine);
    myCoroutineInstance();
  }

  t.Pause();
  HILOG(kInfo, "Latency: {} MOps", ops / t.GetUsec());
}