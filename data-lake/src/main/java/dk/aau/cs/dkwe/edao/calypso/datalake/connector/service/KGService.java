package dk.aau.cs.dkwe.edao.calypso.datalake.connector.service;

import com.google.gson.*;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Entrypoint for communicating with KG service
 */
public class KGService extends Service
{
    private static final String KG_SERVICE_DIR = "/home/knowledge-graph/neo4j";

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

            int responseCode = (int) comm.send(content.toString(), headers);

            if (responseCode != HttpStatus.OK.value())
            {
                throw new RuntimeException("Received response code " + responseCode + " when requesting entity types from EKG Manager");
            }

            String received = (String) comm.receive();
            JsonElement parsed = JsonParser.parseString(received);
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
            Files.move(dir.toPath(), new File(KG_SERVICE_DIR).toPath(), StandardCopyOption.REPLACE_EXISTING);

            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), "insert-links");
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

            JsonObject folder = new JsonObject();
            folder.add("folder", new JsonPrimitive(KG_SERVICE_DIR));
            int responseCode = (int) comm.send(folder.toString(), headers);

            return responseCode == HttpStatus.OK.value();
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
}
