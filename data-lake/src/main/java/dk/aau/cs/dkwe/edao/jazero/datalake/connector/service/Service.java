package dk.aau.cs.dkwe.edao.jazero.datalake.connector.service;

import dk.aau.cs.dkwe.edao.jazero.communication.Communicator;
import dk.aau.cs.dkwe.edao.jazero.communication.ServiceCommunicator;

import java.net.MalformedURLException;

public abstract class Service
{
    private final String host;
    private final int port;

    protected Service(String host, int port, boolean testConnection)
    {
        this.host = host;
        this.port = port;

        if (testConnection && !testConnection())
        {
            throw new RuntimeException("Could not connect to service '" + this.host + ":" + this.port +
                "'. Make sure the service is running.");
        }
    }

    protected String getHost()
    {
        return this.host;
    }

    protected int getPort()
    {
        return this.port;
    }

    public boolean testConnection()
    {
        try
        {
            Communicator comm = ServiceCommunicator.init(this.host, this.port, "ping");
            return comm.testConnection();
        }

        catch (MalformedURLException e)
        {
            return false;
        }
    }
}
