package dk.aau.cs.dkwe.edao.calypso.datalake.connector.service;

import com.google.gson.*;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.Response;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

/**
 * Entrypoint for communicating with KG service
 */
public class KGService extends Service
{
    public KGService(String host, int port)
    {
        super(host, port);
    }

    private Response performSend(String content, String mapping, String malformedUrlMsg, String ioExceptionMsg, boolean throwExceptions)
    {
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), mapping);
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

            Response response = comm.send(content, headers);

            if (response.getResponseCode() != HttpStatus.OK.value())
            {
                throw new RuntimeException("Received response code " + response.getResponseCode());
            }

            return response;
        }

        catch (MalformedURLException e)
        {
            if (throwExceptions)
            {
                throw new RuntimeException(malformedUrlMsg + ": " + e.getMessage());
            }

            return null;
        }

        catch (IOException e)
        {
            if (throwExceptions)
            {
                throw new RuntimeException(ioExceptionMsg + ": " + e.getMessage());
            }

            return null;
        }
    }

    public List<String> searchTypes(String entity)
    {
        JsonObject content = new JsonObject();
        content.add("entity", new JsonPrimitive(entity));
        Response response = performSend(content.toString(), "types",
                "URL for KG service to retrieve entity types is malformed",
                "IOException when sending POST request for entity types", true);
        JsonElement parsed = JsonParser.parseString((String) response.getResponse());
        JsonArray array = parsed.getAsJsonObject().getAsJsonArray("types").getAsJsonArray();
        List<String> types = new ArrayList<>();

        for (JsonElement element : array)
        {
            types.add(element.getAsString());
        }

        return types;
    }

    public List<String> searchPredicates(String entity)
    {
        JsonObject content = new JsonObject();
        content.add("entity", new JsonPrimitive(entity));
        Response response = performSend(content.toString(), "predicates",
                "URL for KG service to retrieve entity types is malformed",
                "IOException when sending POST request for entity types", true);
        JsonElement parsed = JsonParser.parseString((String) response.getResponse());
        JsonArray array = parsed.getAsJsonObject().getAsJsonArray("predicates").getAsJsonArray();
        List<String> predicates = new ArrayList<>();

        for (JsonElement element : array)
        {
            predicates.add(element.getAsString());
        }

        return predicates;
    }

    public boolean insertLinks(File dir)
    {
        JsonObject folder = new JsonObject();
        folder.add("folder", new JsonPrimitive(dir.getAbsolutePath()));
        Response response = performSend(folder.toString(), "insert-links",
                "Malformed URL when inserting links",
                "IOException when inserting links", false);

        if (response == null)
        {
            return false;
        }

        return response.getResponseCode() == HttpStatus.OK.value();
    }

    public long size()
    {
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), "size");
            return Long.parseLong((String) comm.receive());
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("URL for EKG Manager to get EKG size is malformed: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("IOException when sending GET request to get size of EKG: " + e.getMessage());
        }
    }

    public String getFromWikiLink(String wikiLink)
    {
        JsonObject wikiURL = new JsonObject();
        wikiURL.add("wiki", new JsonPrimitive(wikiLink));
        Response response = performSend(wikiURL.toString(), "from-wiki-link",
                "URL for EKG Manager to retrieve entity link is malformed",
                "IOException when sending POST request to get entity link", true);
        String entity = (String) response.getResponse();

        if (response.getResponseCode() != HttpStatus.OK.value())
        {
            return null;
        }

        return !entity.equals("None") ? entity : null;
    }

    public Map<String, Set<String>> getSubGraph()
    {
        try
        {
            Map<String, Set<String>> subGraph = new HashMap<>();
            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), "sub-kg");
            JsonElement parsed = JsonParser.parseString((String) comm.receive());
            JsonArray array = parsed.getAsJsonObject().getAsJsonArray("entities").getAsJsonArray();

            for (JsonElement element : array)
            {
                JsonObject entity = element.getAsJsonObject();
                Set<String> objects = new HashSet<>();

                for (JsonElement objectElement : entity.getAsJsonArray("objects").getAsJsonArray())
                {
                    objects.add(objectElement.getAsString());
                }

                subGraph.put(entity.get("entity").getAsString(), objects);
            }

            return subGraph;
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("URL for EKG Manager to retrieve entity link is malformed: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("IOException when sending GET request to get sub-KG: " + e.getMessage());
        }
    }
}
