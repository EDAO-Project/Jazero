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

    public List<String> searchTypes(String entity)
    {
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), "types");
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

            JsonObject content = new JsonObject();
            content.add("entity", new JsonPrimitive(entity));

            Response response = comm.send(content.toString(), headers);

            if (response.getResponseCode() != HttpStatus.OK.value())
            {
                throw new RuntimeException("Received response code " + response.getResponseCode() +
                        " when requesting entity types from EKG Manager");
            }

            JsonElement parsed = JsonParser.parseString((String) response.getResponse());
            JsonArray array = parsed.getAsJsonObject().getAsJsonArray("types").getAsJsonArray();
            List<String> types = new ArrayList<>();

            for (JsonElement element : array)
            {
                types.add(element.getAsString());
            }

            return types;
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("URL for KG service to retrieve entity types is malformed: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("IOException when sending POST request for entity types: " + e.getMessage());
        }
    }

    public boolean insertLinks(File dir)
    {
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), "insert-links");
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

            JsonObject folder = new JsonObject();
            folder.add("folder", new JsonPrimitive(dir.getAbsolutePath()));

            Response response = comm.send(folder.toString(), headers);
            return response.getResponseCode() == HttpStatus.OK.value();
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("URL for EKG Manager to retrieve entity types is malformed: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("IOException when sending POST request to insert table links: " + e.getMessage());
        }
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
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), "from-wiki-link");
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

            JsonObject wikiURL = new JsonObject();
            wikiURL.add("wiki", new JsonPrimitive(wikiLink));

            Response response = comm.send(wikiURL.toString(), headers);
            String entity = (String) response.getResponse();

            if (response.getResponseCode() != HttpStatus.OK.value())
            {
                return null;
            }

            return !entity.equals("None") ? entity : null;
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("URL for EKG Manager to retrieve entity link is malformed: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("IOException when sending POST request to get entity link: " + e.getMessage());
        }
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
