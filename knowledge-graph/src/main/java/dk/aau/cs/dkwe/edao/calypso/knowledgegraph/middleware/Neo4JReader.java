package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware;

import dk.aau.cs.dkwe.edao.calypso.datalake.loader.IndexIO;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Type;
import dk.aau.cs.dkwe.edao.calypso.knowledgegraph.connector.Neo4jEndpoint;
import org.neo4j.driver.*;

import javax.el.PropertyNotFoundException;
import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reads KG by exporting from Neo4J instance
 * Output is stored in kg.ttl
 *
 * WARNING
 *      Using the RDF export procedure in Neo4J is an experimental feature which may change in the future
 *      - https://neo4j.com/labs/neosemantics/tutorial/#_using_the_cypher_n10s_rdf_export_procedure
 */
public class Neo4JReader extends Neo4JHandler implements IndexIO
{
    private static final String KG_FILE = Neo4JHandler.HOME_IMPORT +  "kg.ttl";
    private static final String EXPORT_SCRIPT = Neo4JHandler.BASE + "export.sh";

    @Override
    public void performIO() throws IOException
    {
        if (!new File(KG_FILE).exists())
        {
            throw new RuntimeException("KG file does not exist: Make sure to insert graph first");
        }
    }

    public String getGraphFile()
    {
        if (!new File(KG_FILE).exists())
        {
            throw new RuntimeException("KG file does not exist: Make sure to insert graph first");
        }

        return KG_FILE;
    }

    public List<Type> entityTypes(Entity entity)
    {
        try (Neo4jEndpoint endpoint = new Neo4jEndpoint(CONFIG_FILE))
        {
            List<String> typesStr = endpoint.searchTypes(entity.getUri());
            return typesStr.stream().map(Type::new).collect(Collectors.toList());
        }

        catch (IOException e)
        {
            return null;
        }
    }
}
