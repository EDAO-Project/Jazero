package dk.aau.cs.dkwe.edao.calypso.datalake.connector.service;

import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;

import java.net.MalformedURLException;

public abstract class Service
{
    private String host;
    private int port;

    protected Service(String host, int port)
    {
        this.host = host;
        this.port = port;

        if (!testConnection())
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
