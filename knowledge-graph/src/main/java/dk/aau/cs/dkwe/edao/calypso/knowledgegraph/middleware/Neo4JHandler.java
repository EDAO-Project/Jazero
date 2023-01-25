package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware;

import dk.aau.cs.dkwe.edao.calypso.knowledgegraph.connector.Neo4jEndpoint;

import java.io.File;
import java.io.IOException;

public abstract class Neo4JHandler
{
    protected static final String BASE = "neo4j/";
    protected static final String HOME = BASE + "neo4j-server/";
    protected static final String HOME_IMPORT = HOME + "import/";
    protected static final String KG_DIR = BASE + "kg/";
    protected static final String CONFIG_FILE = BASE + "config.properties";

    public static Neo4jEndpoint getConnector() throws IOException
    {
        return new Neo4jEndpoint(CONFIG_FILE);
    }
}
