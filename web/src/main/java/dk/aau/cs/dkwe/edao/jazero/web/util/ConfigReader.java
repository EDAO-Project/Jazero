package dk.aau.cs.dkwe.edao.jazero.web.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class ConfigReader
{
    private static Map<String, Map<String, String>> config;
    private static final String CONFIG_FILENAME = "config.json";

    static
    {
        try
        {
            readConfig();
        }

        catch (IOException ignored)
        {
            System.out.println("Error: " + ignored.getMessage());
        }
    }

    private static void readConfig() throws IOException
    {
        config = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(new File(CONFIG_FILENAME));
        JsonNode dataLakesNode = node.get("data-lakes");
        dataLakesNode.forEach(dataLake -> {
            String name = dataLake.get("name").asText(), ip = dataLake.get("ip").asText();
            JsonNode loginNode = dataLake.get("login");
            String username = loginNode.get("username").asText(),
                    password = loginNode.get("password").asText();
            Map<String, String> dataLakeMap = new HashMap<>();
            dataLakeMap.put("ip", ip);
            dataLakeMap.put("username", username);
            dataLakeMap.put("password", password);
            config.put(name, dataLakeMap);
        });
    }

    public static Set<String> dataLakes()
    {
        return config.keySet();
    }

    public static String getIp(String dataLake)
    {
        return get(dataLake, "ip");
    }

    public static String getUsername(String dataLake)
    {
        return get(dataLake, "username");
    }

    public static String getPassword(String dataLake)
    {
        return get(dataLake, "password");
    }

    private static String get(String dataLake, String key)
    {
        if (!config.containsKey(dataLake))
        {
            return null;
        }

        return config.get(dataLake).get(key);
    }
}
