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
apt install cmake curl libcurl4-gnutls-dev -y
```

## Building Static Library
To build the static library file `libjdlc.a`, run the following commands.

```bash
mkdir -p build lib
cmake -DCMAKE_BUILD_TYPE=Release -B build/
cmake --build build/ --target jdlc -j 6
```

Now, the library file can be found in `lib/libjdlc.a`.