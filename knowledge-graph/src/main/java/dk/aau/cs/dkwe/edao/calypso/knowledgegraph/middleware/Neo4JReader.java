package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware;

import dk.aau.cs.dkwe.edao.calypso.datalake.loader.IndexIO;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Entity;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.graph.Type;
import org.neo4j.driver.*;

import javax.el.PropertyNotFoundException;
import java.io.*;
import java.nio.file.Files;
import java.util.*;

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
        Driver driver;

        try
        {
            driver = initDriver();
        }

        catch (IOException e)
        {
            throw new RuntimeException("Could not instantiate Neo4J connection: " + e.getMessage());
        }

        Map<String, Object> params = new HashMap<>();
        params.put("entity", entity.getUri());

        Session session = driver.session();
        List<Type> types = session.readTransaction(tx -> {
            Result result = tx.run("MATCH (a:Resource) -[l:rdf__type]-> (b:Resource)\n"
                    + "WHERE a.uri in [$entity]\n"
                    + "RETURN b.uri as mention", params);
            return result.list().stream().map(r -> new Type(r.get("mention").asString())).toList();
        });
        session.close();

        return types;
    }

    private Driver initDriver() throws IOException
    {
        File confFile = new File(Neo4JHandler.CONFIG_FILE);

        if (!confFile.exists())
        {
            throw new FileNotFoundException("Config file for Neo4J instance does not exist in knowledge-graph/neo4/");
        }

        Properties props = new Properties();
        InputStream stream = Files.newInputStream(confFile.toPath());
        props.load(stream);
        stream.close();

        String dbUri = props.getProperty("neo4j.uri"),
                dbUser = props.getProperty("neo4j.user"),
                dbPassword = props.getProperty("neo4j.password");

        if (dbUri == null || dbUser == null || dbPassword == null)
        {
            throw new PropertyNotFoundException("Missing properties in Neo4J configuration file (" + Neo4JHandler.CONFIG_FILE + ")");
        }

        return GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword));
    }
}
