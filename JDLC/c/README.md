# JDLC C Connector
This is the Jazero C connector!

You can install the tool using apt as follows:

```bash
apt install <INSERT_NAME>
```

## Building the Tool
Here is the instructions to manually build the tool and generate a static `.a` file.

### Prerequisites
To build the project, you only need to install CMake and Curl.

```bash
apt install git cmake curl libcurl4-gnutls-dev -y
```

## Building Static Library
To build the static library file `jdlc-core.a`, run the following commands.

```bash
mkdir -p build lib
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build ./ --target all -- -j 6
```

Now, the library file can be found in `lib/libjdlc.a`.