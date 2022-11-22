package dk.aau.cs.dkwe.edao.calypso.datalake.connector.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.Response;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.TableDeserializer;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.TableSerializer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

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
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), "link");
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

            JsonObject content = new JsonObject();
            content.add("input", new JsonPrimitive(tableEntity));

            Response response = comm.send(content.toString(), headers);

            if (response.getResponseCode() != HttpStatus.OK.value())
            {
                throw new RuntimeException("Received response code " + response.getResponseCode() +
                        " when requesting entity link from entity linker");
            }

            String entity = (String) response.getResponse();
            return !entity.equals("None") ? entity : null;
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("URL for entity linking service to retrieve entity link is malformed: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("IOException when sending POST request for entity link: " + e.getMessage());
        }
    }

    public Table<String> linkTable(Table<String> table)
    {
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), getPort(), "link-table");
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

            String serialized = TableSerializer.create(table).serialize();
            JsonObject content = new JsonObject();
            content.add("table", new JsonPrimitive(serialized));

            Response response = comm.send(content.toString(), headers);

            if (response.getResponseCode() != HttpStatus.OK.value())
            {
                throw new RuntimeException("Received response code " + response.getResponseCode() +
                        " when requesting table entity links from entity linker");
            }

            return TableDeserializer.create((String) response.getResponse()).deserialize();
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("URL for entity linking service to retrieve entity links from table is malformed: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("IOException when sending POST request for table entity links: " + e.getMessage());
        }
    }
}
