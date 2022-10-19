package dk.aau.cs.dkwe.edao.calypso.communication;

import java.io.IOException;
import java.util.Map;

public interface Communicator
{
    boolean testConnection();
    Response send(Object content, Map<String, String> headers) throws IOException;
    Object receive() throws IOException;
    Object receive(Map<String, String> headers) throws IOException;
}
