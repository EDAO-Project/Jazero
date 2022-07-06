package dk.aau.cs.dkwe.edao.calypso.communication;

import java.io.IOException;

public interface Communicator
{
    boolean testConnection();
    Object send(Object content) throws IOException;
    Object receive() throws IOException;
}
