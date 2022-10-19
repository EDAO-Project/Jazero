package dk.aau.cs.dkwe.edao.calypso.entitylinker.link;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Uses DBpedia lookup service to find DBpedia entities and check for existence according to the EKG Manager
 */
public class DBpediaLookupEntityLinker implements EntityLink<String, String>
{
    public static DBpediaLookupEntityLinker make()
    {
        return new DBpediaLookupEntityLinker();
    }

    private DBpediaLookupEntityLinker() {}

    /**
     * Performs a lookup query using the DBpedia lookup service and check the EKG Manager for existence of the entity
     * @param key Entity to perform lookup with
     * @return Linked entity
     */
    @Override
    public String link(String key)
    {
        try
        {
            String mapping = "/api/search/KeywordSearch?&QueryString=" + key.replace(" ", "%20");
            Communicator comm = ServiceCommunicator.init("lookup.dbpedia.org", mapping, true);
            Map<String, String> headers = new HashMap<>();
            headers.put("Accept:", "application/json");

            JsonElement json = JsonParser.parseString((String) comm.receive(headers));
            return "Test";
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("Error with Wikipedia URL: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("Error when reading Wikipedia response: " + e.getMessage());
        }
    }
}
