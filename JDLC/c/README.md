# JDLC C Connector
This is the Jazero C connector!

## Prerequisites
To build the project, you only need to install CMake.

```bash
apt install cmake -y
```

## Building Static Library
To build the static library file `libjdlc.a`, run the following commands.

```bash
mkdir -p build lib
cmake -DCMAKE_BUILD_TYPE=Release -B build/
cmake --build build/ --target jdlc -j 6
```

Now, the library file can be found in `lib/libjdlc.a`.