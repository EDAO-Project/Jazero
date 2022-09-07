package dk.aau.cs.dkwe.edao.calypso.datalake.connector.service;

/**
 * Entrypoint for communicating with entity linker service
 */
public class ELService extends Service
{
    public ELService(String host, int port)
    {
        super(host, port);
    }

    public String link(String tableEntity)
    {
        return "null";
    }
}
