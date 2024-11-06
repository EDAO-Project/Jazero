package dk.aau.cs.dkwe.edao.jazero.web.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class ConfigReader
{
    private static Map<String, String> dataLakes;
    private static final String CONFIG_FILENAME = "web/config.json";

    static
    {
        try
        {
            readConfig();
        }

        catch (IOException ignored) {}
    }

    private static void readConfig() throws IOException
    {
        dataLakes = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(new File(CONFIG_FILENAME));
        JsonNode dataLakesNode = node.get("data-lakes");
        dataLakesNode.forEach(dataLake -> {
            Iterator<String> dataLakeNames = dataLake.fieldNames();

            while (dataLakeNames.hasNext())
            {
                String dataLakeName = dataLakeNames.next();
                String ip = dataLake.get(dataLakeName).asText();
                dataLakes.put(dataLakeName, ip);
            }
        });
    }

    public static Map<String, String> getDataLakes()
    {
        return dataLakes;
    }
}
