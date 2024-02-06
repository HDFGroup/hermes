# Hermes

Hermes is a heterogeneous-aware, multi-tiered, dynamic, and distributed I/O 
buffering system that aims to significantly accelerate I/O performance. 
See the [official site](http://www.cs.iit.edu/~scs/assets/projects/Hermes/Hermes.html) for more information. For design documents, 
architecture description, performance data, and individual component design, 
see the [wiki](https://grc.iit.edu/docs/category/hermes).

![Build](https://github.com/HDFGroup/hermes/workflows/GitHub%20Actions/badge.svg)

[![Coverage Status](https://coveralls.io/repos/github/HDFGroup/hermes/badge.svg?branch=master)](https://coveralls.io/github/HDFGroup/hermes?branch=master)

## Building

Read the guide on [Building Hermes](https://grc.iit.edu/docs/hermes/building-hermes).

## Contributing

We follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html). You can run `make lint` to ensure that your code conforms to the style. This requires the `cpplint` Python module (`pip install cpplint`). Alternatively, you can let the CI build inform you of required style changes.
