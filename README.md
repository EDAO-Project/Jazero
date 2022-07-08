# Calypso
Welcome to Calypso: A semantic data lake microservice architecture for semantically augmented table search

## Starting Calypso
Setting up and running Calypso is very simple.
All you need is Docker-compose installed.

Start Calypso with the following simple command

```bash
docker-compose up
```

Now, Calypso is accessible on localhost or on the machine's IP address.

## Working with Calypso
[Here will be some instructions on using the CDLC driver and maybe also the API if we build that]

## Setting Up Calypso in an IDE
Most of the components in Calypso are dependent on the `communication` module.
Therefore, enter this module and run the following to install it as a dependency

```bash
cd communication
mvn clean install
```