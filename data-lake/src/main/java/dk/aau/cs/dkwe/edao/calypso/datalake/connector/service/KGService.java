package dk.aau.cs.dkwe.edao.calypso.datalake.connector.service;

import com.google.gson.*;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

            Object received = comm.send(content.toString(), headers);
            JsonElement parsed = JsonParser.parseString((String) received);
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
}
