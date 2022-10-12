package dk.aau.cs.dkwe.edao.calypso.entitylinker.link;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;
import dk.aau.cs.dkwe.edao.calypso.datalake.connector.service.KGService;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;

import java.io.IOException;
import java.net.MalformedURLException;

public class GoogleKGEntityLinker implements EntityLink<String, String>
{
    public static EntityLink<String, String> make()
    {
        return new GoogleKGEntityLinker();
    }

    private GoogleKGEntityLinker() {}

    /**
     * Search Google KG API for Wikipedia URL of entity
     * @param entity Input entity to be linked to KG entity
     * @return KG entity
     */
    @Override
    public String link(String entity)
    {
        try
        {
            String apiKey = Configuration.getGoogleAPIKey();
            String mapping = "/v1/entities:search?query=taylor+swift&key=" + apiKey + "&limit=1&indent=True";
            Communicator comm = ServiceCommunicator.init("kgsearch.googleapis.com", mapping, true);
            JsonElement json = JsonParser.parseString((String) comm.receive());

            if (json == null || !json.getAsJsonObject().has("itemListElement"))
            {
                return null;
            }

            JsonArray array = json.getAsJsonObject().get("itemListElement").getAsJsonArray();

            if (array.size() == 0)
            {
                return null;
            }

            String wikiUrl = array.get(0).getAsJsonObject().get("result")
                                .getAsJsonObject().get("detailedDescription")
                                .getAsJsonObject().get("url").getAsString();
            KGService kgService = new KGService(Configuration.getEKGManagerHost(), Configuration.getEKGManagerPort());
            return kgService.getFromWikiLink(wikiUrl);
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("Error with Google KG API URL: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("Error when reading Google KG API response: " + e.getMessage());
        }
    }
}
