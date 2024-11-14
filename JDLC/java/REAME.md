# JDLC Java Connector
This is the Jazero Java connector!
Use it in a Java application to interact with a running Jazero instance.

## Building Jar File
From the project root directory, enter the `data-lake/` directory and run the following command:

```bash
mvn clean install
```

Run the same command in the `communication/` directory.

Now, re-enter this directory in `JDLC/java/` and run the following command to build the jar file for this connector:

```bash
mvn clean package
```

You can now add this .jar file in `target/` as a dependency in your application.