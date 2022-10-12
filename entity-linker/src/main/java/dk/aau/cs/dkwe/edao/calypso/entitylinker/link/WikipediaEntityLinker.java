package dk.aau.cs.dkwe.edao.calypso.entitylinker.link;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.Set;

/**
 * Searches Wikipedia for Wiki entity and looks in the EKG Manager for entities that link to that Wiki entity
 */
public class WikipediaEntityLinker implements EntityLink<String, String>
{
    public static EntityLink<String, String> make()
    {
        return new WikipediaEntityLinker();
    }

    /**
     * 1. Search https://en.wikipedia.org/w/api.php?action=query&origin=*&format=json&generator=search&gsrnamespace=0&gsrlimit=5&gsrsearch=<QUERY>
     *    for Wikipedia results.
     * 2. Get the first result and search https://en.wikipedia.org/w/api.php?action=query&prop=info&pageids=<PAGE ID>&inprop=url
     *    for the full Wikipedia URL for the result Wikipedia page ID.
     * 3. Get the KG entity that has a link to the found Wikipedia entity in the EKG Manager.
     * @param entity
     * @return
     */
    @Override
    public String link(String entity)
    {
        String mapping = "/w/api.php?action=query&origin=*&format=json&generator=search&gsrnamespace=0&gsrlimit=5&gsrsearch=";
        mapping += "'" + entity.replace(' ', '_') + "'";

        try
        {
            Communicator comm = ServiceCommunicator.init("en.wikipedia.org", mapping, true);
            JsonElement json = JsonParser.parseString((String) comm.receive());

            if (json == null || !json.getAsJsonObject().has("query"))
            {
                return null;
            }

            Set<Map.Entry<String, JsonElement>> entrySet =
                    json.getAsJsonObject().get("query").getAsJsonObject().get("pages").getAsJsonObject().entrySet();
            String pageId = getFirstPageId(entrySet);

            if (pageId == null)
            {
                throw new RuntimeException("Entity '" + entity + "' did not return any Wikipedia results");
            }

            mapping = "/w/api.php?action=query&format=json&prop=info&pageids=" + pageId + "&inprop=url";
            comm = ServiceCommunicator.init("en.wikipedia.org", mapping, true);
            json = JsonParser.parseString((String) comm.receive());

            String url = json.getAsJsonObject()
                    .get("query").getAsJsonObject()
                    .get("pages").getAsJsonObject()
                    .get(pageId).getAsJsonObject()
                    .get("fullurl").getAsString();
            KGService kgService = new KGService(Configuration.getEKGManagerHost(), Configuration.getEKGManagerPort());
            return kgService.getFromWikiLink(url);
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

    private static String getFirstPageId(Set<Map.Entry<String, JsonElement>> pageIds)
    {
        for (Map.Entry<String, JsonElement> entry : pageIds)
        {
            if (entry.getValue().getAsJsonObject().get("index").getAsInt() == 1)
            {
                return entry.getKey();
            }
        }

        return null;
    }
}
