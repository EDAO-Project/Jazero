package dk.aau.cs.dkwe.edao.calypso.datalake.system;

import dk.aau.cs.dkwe.edao.calypso.storagelayer.StorageHandler;

import java.io.*;
import java.util.Properties;

public class Configuration
{
    private static class ConfigurationIO
    {
        private InputStream input;
        private OutputStream output;

        ConfigurationIO(InputStream input)
        {
            this.input = input;
            this.output = null;
        }

        ConfigurationIO(OutputStream output)
        {
            this.output = output;
            this.input = null;
        }

        void save(Properties properties)
        {
            if (this.output == null)
                throw new UnsupportedOperationException("No output stream class provided");

            try (ObjectOutputStream objectOutput = new ObjectOutputStream(this.output))
            {
                objectOutput.writeObject(properties);
                objectOutput.flush();
            }

            catch (IOException e)
            {
                throw new RuntimeException("IOException when saving configuration: " + e.getMessage());
            }
        }

        Properties read()
        {
            if (this.input == null)
                throw new UnsupportedOperationException("No input stream class provided");

            try (ObjectInputStream objectInput = new ObjectInputStream(this.input))
            {
                return (Properties) objectInput.readObject();
            }

            catch (IOException | ClassNotFoundException e)
            {
                throw new RuntimeException("Exception when reading configuration: " + e.getMessage());
            }
        }
    }

    private static final File CONF_FILE = new File(".config.conf");

    static
    {
        addDefaults();
    }

    public static void reloadConfiguration()
    {
        addDefaults();
    }

    private static void addDefaults()
    {
        Properties props = readProperties();

        if (!props.contains("EntityTable"))
            props.setProperty("EntityTable", "entity_table.ser");

        if (!props.contains("EntityLinker"))
            props.setProperty("EntityLinker", "entity_linker.ser");

        if (!props.contains("EntityToTables"))
            props.setProperty("EntityToTables", "entity_to_tables.ser");

        if (!props.contains("TableToEntities"))
            props.setProperty("TableToEntities", "tableIDToEntities.ttl");

        if (!props.contains("TableToTypes"))
            props.setProperty("TableToTypes", "tableIDToTypes.ttl");

        if (!props.contains("WikiLinkToEntitiesFrequency"))
            props.setProperty("WikiLinkToEntitiesFrequency", "wikilinkToNumEntitiesFrequency.json");

        if (!props.contains("CellToNumLinksFrequency"))
            props.setProperty("CellToNumLinksFrequency", "cellToNumLinksFrequency.json");

        if (!props.contains("TableStats"))
            props.setProperty("TableStats", "perTableStats.json");

        if (!props.contains("LogLevel"))
            props.setProperty("LogLevel", Logger.Level.INFO.toString());

        if (!props.contains("DBName"))
            props.setProperty("DBName", "embeddings");

        if (!props.contains("DBUsername"))
            props.setProperty("DBUsername", "calypso");

        if (!props.contains("DBPassword"))
            props.setProperty("DBPassword", "1234");

        if (!props.contains("SDLHost"))
            props.setProperty("SDLHost", "localhost");

        if (!props.contains("SDLPort"))
            props.setProperty("SDLPort", "8081");

        if (!props.contains("EntityLinkerHost"))
            props.setProperty("EntityLinkerHost", "localhost");

        if (!props.contains("EntityLinkerPort"))
            props.setProperty("EntityLinkerPort", "8082");

        if (!props.contains("EKGManagerHost"))
            props.setProperty("EKGManagerHost", "localhost");

        if (!props.contains("EKGManagerPort"))
            props.setProperty("EKGManagerPort", "8083");

        if (!props.contains("GoogleAPIKey"))
            props.setProperty("GoogleAPIKey", "AIzaSyB9mH-706htjAcFBxfrXaJ5jpDnuBfxhm8");

        if (!props.contains("LuceneDir"))
            props.setProperty("LuceneDir", "./lucene");

        if (!props.contains("KGDir"))
            props.setProperty("KGDir", "/home/knowledge-graph/neo4j");

        writeProperties(props);
    }

    private static synchronized Properties readProperties()
    {
        try
        {
            return (new ConfigurationIO(new FileInputStream(CONF_FILE))).read();
        }

        catch (FileNotFoundException | RuntimeException e)
        {
            return new Properties();
        }
    }

    private static synchronized void writeProperties(Properties properties)
    {
        try
        {
            (new ConfigurationIO(new FileOutputStream(CONF_FILE))).save(properties);
        }

        catch (FileNotFoundException e) {}
    }

    private static void addProperty(String key, String value)
    {
        Properties properties = readProperties();
        properties.setProperty(key, value);
        writeProperties(properties);
    }

    public static void setDBPath(String path)
    {
        addProperty("DBPath", path);
    }

    public static String getDBPath()
    {
        return readProperties().getProperty("DBPath");
    }

    public static void setDBName(String name)
    {
        addProperty("DBName", name);
    }

    public static String getDBName()
    {
        return readProperties().getProperty("DBName");
    }

    public static void setDBHost(String host)
    {
        addProperty("DBHost", host);
    }

    public static String getDBHost()
    {
        return readProperties().getProperty("DBHost");
    }

    public static void setDBPort(int port)
    {
        addProperty("DBPort", String.valueOf(port));
    }

    public static int getDBPort()
    {
        return Integer.parseInt(readProperties().getProperty("DBPort"));
    }

    public static void setDBUsername(String username)
    {
        addProperty("DBUsername", username);
    }

    public static String getDBUsername()
    {
        return readProperties().getProperty("DBUsername");
    }

    public static void setDBPassword(String password)
    {
        addProperty("DBPassword", password);
    }

    public static String getDBPassword()
    {
        return readProperties().getProperty("DBPassword");
    }

    public static void setLargestId(String id)
    {
        addProperty("LargestID", id);
    }

    public static String getLargestId()
    {
        return readProperties().getProperty("LargestID");
    }

    public static String getEntityTableFile()
    {
        return readProperties().getProperty("EntityTable");
    }

    public static String getEntityLinkerFile()
    {
        return readProperties().getProperty("EntityLinker");
    }

    public static String getEntityToTablesFile()
    {
        return readProperties().getProperty("EntityToTables");
    }

    public static String getTableToEntitiesFile()
    {
        return readProperties().getProperty("TableToEntities");
    }

    public static String getTableToTypesFile()
    {
        return readProperties().getProperty("TableToTypes");
    }

    public static String getWikiLinkToEntitiesFrequencyFile()
    {
        return readProperties().getProperty("WikiLinkToEntitiesFrequency");
    }

    public static String getCellToNumLinksFrequencyFile()
    {
        return readProperties().getProperty("CellToNumLinksFrequency");
    }

    public static String getTableStatsFile()
    {
        return readProperties().getProperty("TableStats");
    }

    public static void setLogLevel(Logger.Level level)
    {
        addProperty("LogLevel", level.toString());
    }

    public static String getLogLevel()
    {
        return readProperties().getProperty("LogLevel");
    }

    public static void setIndexesLoaded(boolean value)
    {
        addProperty("IndexesLoaded", String.valueOf(value));
    }

    public static boolean areIndexesLoaded()
    {
        return Boolean.parseBoolean(readProperties().getProperty("IndexesLoaded"));
    }

    public static void setEmbeddingsLoaded(boolean value)
    {
        addProperty("EmbeddingsLoaded", String.valueOf(value));
    }

    public static boolean areEmbeddingsLoaded()
    {
        return Boolean.parseBoolean(readProperties().getProperty("EmbeddingsLoaded"));
    }

    public static void setStorageType(StorageHandler.StorageType type)
    {
        addProperty("StorageType", type.name());
    }

    public static StorageHandler.StorageType getStorageType()
    {
        return StorageHandler.StorageType.valueOf(readProperties().getProperty("StorageType"));
    }

    public static void setSDLManagerHost(String host)
    {
        addProperty("SDLHost", host);
    }

    public static String getSDLManagerHost()
    {
        return readProperties().getProperty("SDLHost");
    }

    public static void setSDLManagerPort(int port)
    {
        addProperty("SDLPort", String.valueOf(port));
    }

    public static int getSDLManagerPort()
    {
        return Integer.parseInt(readProperties().getProperty("SDLPort"));
    }

    public static void setEntityLinkerHost(String host)
    {
        addProperty("EntityLinkerHost", host);
    }

    public static String getEntityLinkerHost()
    {
        return readProperties().getProperty("EntityLinkerHost");
    }

    public static void setEntityLinkerPort(int port)
    {
        addProperty("EntityLinkerPort", String.valueOf(port));
    }

    public static int getEntityLinkerPort()
    {
        return Integer.parseInt(readProperties().getProperty("EntityLinkerPort"));
    }

    public static void setEKGManagerHost(String host)
    {
        addProperty("EKGManagerHost", host);
    }

    public static String getEKGManagerHost()
    {
        return readProperties().getProperty("EKGManagerHost");
    }

    public static void setEKGManagerPort(int port)
    {
        addProperty("EKGManagerPort", String.valueOf(port));
    }

    public static int getEKGManagerPort()
    {
        return Integer.parseInt(readProperties().getProperty("EKGManagerPort"));
    }

    public static String getGoogleAPIKey()
    {
        return readProperties().getProperty("GoogleAPIKey");
    }

    public static void setLuceneDir(String dir)
    {
        addProperty("LuceneDir", dir);
    }

    public static String getLuceneDir()
    {
        return readProperties().getProperty("LuceneDir");
    }

    public static String getKGDir()
    {
        return readProperties().getProperty("KGDir");
    }
}
