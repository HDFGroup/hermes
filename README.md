The goal of this repo is to have a few internal iterations before we go on
GitHub.

We follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html).
(I'm open to other suggestions.)

# Notes

  * I abandoned the term 'vBucket' and replaced it by 'trait.' I find it too
    confusing.
  * I haven't made up my mind RE: handles vs. objects. Let's keep an open mind
    unless one of you has arguments / experience / strong feelings in a
    particular direction.

# Next Steps

  * Add a few `*.cc` files so that we can compile somthing.
  * Add a `CMakeLists.txt` file so that we can build everything w/ CMake
  * Add logging based on [glog](https://github.com/google/glog).
    (See [How To Use Google Logging Library
    (glog)](http://rpg.ifi.uzh.ch/docs/glog.html).)
  * Think about the use of handles (IDs).
