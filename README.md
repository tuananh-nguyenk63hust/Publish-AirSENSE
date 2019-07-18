Source code made by Tuan Anh Nguyen, SPARCLab HUST

***Install library for Json ( Jsoncpp )

* Ubuntu: 
    Open Terminal:
       
       * sudo apt-get update
       
       * sudo apt-get install libjsoncpp-dev
       
    When complie, add -ljsoncpp.
    
* Windows:  https://github.com/open-source-parsers/jsoncpp

***Install library paho.mqtt.cpp:
  ### Unix and Linux

On *nix systems CMake creates Makefiles.

The build process currently supports a number of Unix and Linux flavors. The build process requires the following tools:

  * CMake v3.5 or newer
  * GCC v4.8 or newer or Clang v3.9 or newer
  * GNU Make

On Debian based systems this would mean that the following packages have to be installed:

```
$ sudo apt-get install build-essential gcc make cmake cmake-gui cmake-curses-gui
```

Building the documentation requires doxygen and optionally graphviz to be installed:

```
$ sudo apt-get install doxygen graphviz
```

First, build and install the Paho C library:

```
$ git clone https://github.com/eclipse/paho.mqtt.c.git
$ cd paho.mqtt.c
$ git checkout v1.2.1
$ cmake -Bbuild -H. -DPAHO_WITH_SSL=ON
$ sudo cmake --build build/ --target install
$ sudo ldconfig
```

This builds with SSL/TLS enabled. If that is not desired, omit the `-DPAHO_WITH_SSL=ON`.

If you installed the C library on a non-standard path, you might want to pass it as value to the `CMAKE_PREFIX_PATH` option.

Using these variables CMake can be used to generate your Makefiles. The out-of-source build is the default on CMake. Therefore it is recommended to invoke all build commands inside your chosen build directory.

An example build session might look like this:

```
$ git clone https://github.com/eclipse/paho.mqtt.cpp
$ cd paho.mqtt.cpp
$ cmake -Bbuild -H. -DPAHO_BUILD_DOCUMENTATION=TRUE -DPAHO_BUILD_SAMPLES=TRUE
$ sudo cmake --build build/ --target install
```

If you did not install Paho C library to a default system location or you want to build against a different version, use the `CMAKE_PREFIX_PATH` to specify its install location:

```
$ cmake -Bbuild -H. -DPAHO_BUILD_DOCUMENTATION=TRUE -DPAHO_BUILD_SAMPLES=TRUE \
    -DCMAKE_PREFIX_PATH=../../paho.mqtt.c/build/install/usr/local ..
```

To use another compiler, either the CXX environment variable can be specified in the configuration step:

```
$ CXX=clang++ cmake ..
```

or the `CMAKE_CXX_COMPILER` flag can be used:


```
$ cmake -DCMAKE_CXX_COMPILER=clang++
```

#### Updating CMake on Ubuntu 14.04 or 16.04

The versions of CMake on Ubuntu 14.04 or 16.04 LTS are pretty old and have some problems with Paho C++ library. A newer version can be added by downloading the source and building it. If the older cmake can be removed from the system using the package manager, or it can be kept, using the Ububtu alternatives to chose between the versions. 

For example, here's how to install CMake v3.6 on Ubuntu 14.04:

```
$ wget http://www.cmake.org/files/v3.6/cmake-3.6.3.tar.gz 
$ tar -xvzf cmake-3.6.3.tar.gz 
$ cd cmake-3.6.3/
$ ./configure
$ make
$ sudo make install
$ sudo mv /usr/bin/cmake /usr/bin/cmake-2.8
$ sudo update-alternatives --install /usr/bin/cmake cmake /usr/local/bin/cmake 100
$ sudo update-alternatives --install /usr/bin/cmake cmake /usr/bin/cmake-2.8 200
$ cmake --version
cmake version 3.6.3
```

You can speed up the CMake build on multi-core systems, by specifying parallel buid jobs for the configure and make steps, above, such as the following for a 4-core system:
```
$ ./configure --parallel=4
$ make -j4
$ sudo make install
```

### Windows

On Windows systems CMake creates Visual Studio project files.

The build process currently supports a number Windows versions. The build process requires the following tools:
  * CMake GUI v3.5 or newer
  * Visual Studio 2015 or newer

First install and open the cmake-gui application. This tutorial is based on cmake-gui 3.5.2.

Second, select the path to the Paho MQTT C library (CMAKE_PREFIX_PATH) if not installed in a standard path. Remember that the Paho MQTT C must be installed on the system. Next, choose if it is supposed to build the documentation (PAHO_BUILD_DOCUMENTATION) and/or the sample applications (PAHO_BUILD_SAMPLES).

Once the configuration is done, click on the Configure button, select the version of the Visual Studio, and then click on Generate button.

At the end of this process you have a Visual Studio solution.

Alternately, the libraries can be completely built at an MSBuild Command Prompt. Download the Paho C and C++ library sources, then open a command window and first compile the Paho C library:

```
> cd paho.mqtt.c
> cmake -Bbuild -H. -DCMAKE_INSTALL_PREFIX=C:\mqtt\paho-c
> cmake --build build/ --target install
```

Then build the C++ library:

```
> cd ..\paho.mqtt.cpp
> cmake -Bbuild -H. -DCMAKE_INSTALL_PREFIX=C:\mqtt\paho-cpp -DPAHO_BUILD_SAMPLES=ON -DPAHO_WITH_SSL=OFF -DCMAKE_PREFIX_PATH=C:\mqtt\paho-c
> cmake --build build/ --target install
```
This builds and installs both libraries to a non-standard location under `C:\mqtt`. Modify this location as desired or use the default location, but either way, the C++ library will most likely need to be told where the C library was built using `CMAKE_PREFIX_PATH`.

It seems quite odd, but even on a 64-bit system using a 64-bit compiler, MSVC seems to default to a 32-bit build target. 

The 64-bit target can be selected using tge CMake generator switch, *-G*, at configuration time. The full version must be provided. For Visual Studio 2015 which is v14 do this to first build the Paho C library:

```
> cmake -G "Visual Studio 14 Win64" -Bbuild -H. -DCMAKE_INSTALL_PREFIX=C:\mqtt\paho-c
...
```

Then use it to build the C++ library:

```
> cmake -G "Visual Studio 14 Win64" -Bbuild -H. -DCMAKE_INSTALL_PREFIX=C:\mqtt\paho-cpp -DPAHO_WITH_SSL=OFF -DCMAKE_PREFIX_PATH=C:\mqtt\paho-c
...
```

*Note that it is very important that you use the same generator (target) to build BOTH libraries, otherwise you will get lots of linker errors when you try to build the C++ library.*


## Example

Sample applications can be found in the source repository at _src/samples_:
https://github.com/eclipse/paho.mqtt.cpp/tree/master/src/samples

This is a partial example of what a typical example might look like:

```cpp
int main(int argc, char* argv[])
{
    sample_mem_persistence persist;
    mqtt::client client(ADDRESS, CLIENTID, &persist);

    callback cb;
    client.set_callback(cb);

    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);

        // First use a message pointer.

        mqtt::message_ptr pubmsg = mqtt::make_message(PAYLOAD1);
        pubmsg->set_qos(QOS);
        client.publish(TOPIC, pubmsg);

        // Now try with itemized publish.

        client.publish(TOPIC, PAYLOAD2, strlen(PAYLOAD2)+1, 0, false);

        // Disconnect
        
        client.disconnect();
    }
    catch (const mqtt::persistence_exception& exc) {
        cerr << "Persistence Error: " << exc.what() << " ["
            << exc.get_reason_code() << "]" << endl;
        return 1;
    }
    catch (const mqtt::exception& exc) {
        cerr << "Error: " << exc.what() << " ["
            << exc.get_reason_code() << "]" << endl;
        return 1;
    }

    return 0;
}
```

-----------

###Json Library for adruino: https://github.com/bblanchon/ArduinoJson

###Mqtt Library for ESP8266: https://github.com/tuanpmt/esp_mqtt
