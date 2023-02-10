package dk.aau.cs.dkwe.edao.calypso.cdlc;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dk.aau.cs.dkwe.edao.calypso.cdlc.query.Query;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.Response;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;
import dk.aau.cs.dkwe.edao.calypso.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.Configuration;
import dk.aau.cs.dkwe.edao.calypso.storagelayer.StorageHandler;
import org.apache.commons.io.FileUtils;
import org.springframework.http.MediaType;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Jazero Data Lake Connector (CDLC)
 * Connector class to communicate with Jazerp
 */
public class CDLC implements Connector
{
    private static final String RELATIVE_MOUNT = "knowledge-graph/neo4j";

    /**
     * Host becomes localhost for this constructor
     */
    public CDLC()
    {
        Configuration.reloadConfiguration();
    }

    public CDLC(String host)
    {
        Configuration.setSDLManagerHost(host);
    }

    /**
     * Send a ping to all services to make sure everything is running
     * @return True if all services are running
     */
    @Override
    public boolean isConnected()
    {
        try
        {
            String serviceHost = Configuration.getSDLManagerHost(); // All services run on the same machine
            Communicator comm = ServiceCommunicator.init(serviceHost, Configuration.getSDLManagerPort(), "/ping");

            if (!comm.testConnection())
            {
                return false;
            }

            comm = ServiceCommunicator.init(serviceHost, Configuration.getEKGManagerPort(), "/ping");

            if (!comm.testConnection())
            {
                return false;
            }

            comm = ServiceCommunicator.init(serviceHost, Configuration.getEntityLinkerPort(), "/ping");
            return comm.testConnection();
        }

        catch (IOException e)
        {
            return false;
        }
    }

    /**
     * Perform table search in Jazero
     * @param k The Top-K value
     * @param scoringType Either use Jaccard of RDF types or embeddings representation of entities using different similarity functions on Cosine similarity
     * @param query The query itself
     * @return Iterators of pairs of tables and their respective scores in descending order
     */
    @Override
    public Iterator<Pair<File, Double>> search(int k, ScoringType scoringType, Query query)
    {
        boolean useEmbeddings = scoringType != ScoringType.TYPE,
                singleColumnPerQueryEntity = true,
                useMaxSimilarityPerColumn = true,
                weightedJaccard = true,
                adjustedJaccard = true;
        TableSearch.CosineSimilarityFunction cosineFunction = scoringType.cosineSimilarityFunction();
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

        JsonObject json = new JsonObject();
        json.addProperty("top-k", String.valueOf(k));
        json.addProperty("use-embeddings", String.valueOf(useEmbeddings));
        json.addProperty("single-column-per-query-entity", String.valueOf(singleColumnPerQueryEntity));
        json.addProperty("use-max-similarity-per-column", String.valueOf(useMaxSimilarityPerColumn));
        json.addProperty("weighted-jaccard", String.valueOf(weightedJaccard));
        json.addProperty("adjusted-jaccard", String.valueOf(adjustedJaccard));
        json.addProperty("cosine-function", cosineFunction.name());
        json.addProperty("similarity-measure", TableSearch.SimilarityMeasure.EUCLIDEAN.name());
        json.addProperty("query", query.toString());

        try
        {
            Communicator comm = ServiceCommunicator.init(Configuration.getSDLManagerHost(), Configuration.getSDLManagerPort(), "/search");
            Response response = comm.send(json.toString(), headers);

            if (response.getResponseCode() != 200)
            {
                throw new RuntimeException("Response code " + response.getResponseCode() + " was returned");
            }

            List<Pair<File, Double>> results = new ArrayList<>();
            JsonArray jsonResults = JsonParser.parseString((String) response.getResponse()).getAsJsonObject()
                    .get("results").getAsJsonArray();
            Iterator<JsonElement> iter = jsonResults.iterator();

            while (iter.hasNext())
            {
                JsonObject element = iter.next().getAsJsonObject();
                results.add(new Pair<>(new File(element.get("table").getAsString()), Double.parseDouble(element.get("score").getAsString())));
            }

            return results.iterator();
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed searching Jazero: " + e.getMessage());
        }
    }

    /**
     * Insertion of JSON tables into Jazero
     * Copies the tables into a directory mounted to a shared Jazero directory
     * This call must be executed locally on the machine hosting Jazero to avoid transferring all files to a remote machine
     * @param tablesDir Directory of tables to be loaded into Jazero
     * @param jazeroDir Directory path to Jazero repository
     * @param storageType Type of storage that the tables will be stored in
     * @param tableEntityPrefix
     * @param kgEntityPrefix
     * @return Indexing statistics
     */
    @Override
    public Map<String, Double> insert(File tablesDir, File jazeroDir, StorageHandler.StorageType storageType, String tableEntityPrefix, String kgEntityPrefix)
    {
        String relativeTablesDir = RELATIVE_MOUNT + "/tables";
        String sharedDir = jazeroDir.getAbsolutePath() + "/" + relativeTablesDir;

        try
        {
            copyDir(tablesDir.getAbsolutePath(), sharedDir);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed copying tables in directory '" + tablesDir + "' to mounted Jazero directory '" + sharedDir + "'");
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);
        headers.put("Storage-Type", storageType.name());

        JsonObject json = new JsonObject();
        json.addProperty("directory", "/home/Jazero/" + relativeTablesDir);
        json.addProperty("table-prefix", tableEntityPrefix);
        json.addProperty("kg-prefix", kgEntityPrefix);

        try
        {
            Communicator comm = ServiceCommunicator.init(Configuration.getSDLManagerHost(), Configuration.getSDLManagerPort(), "/insert");
            Response response = comm.send(json.toString(), headers);
            Map<String, Double> stats = new HashMap<>();

            if (response.getResponseCode() != 200)
            {
                throw new RuntimeException("Response code " + response.getResponseCode() + " was returned");
            }

            String responseStats = (String) response.getResponse();
            stats.put("Loaded tables", Double.valueOf(responseStats.split("Loaded tables: ")[1]));
            stats.put("Index time", Double.valueOf(responseStats.split("Index time: ")[1].split("s")[0]));
            stats.put("Total elapsed time", Double.valueOf(responseStats.split("Total elapsed time: ")[1].split("s")[1]));

            return stats;
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed inserting tables into Jazero: " + e.getMessage());
        }
    }

    private static void copyDir(String src, String target) throws IOException
    {
        if (!new File(target).mkdirs())
        {
            throw new IOException("Could not create new directory '" + target + "'");
        }

        FileUtils.copyDirectory(new File(src), new File(target));
    }

    /**
     * Inserts embeddings into Jazero
     * Embeddings file must be formatted so each entity and its embeddings are on one line
     * Each line starts with the entity following by its embedding values, all separated by the same delimiter (including the entity)
     * This call must be executed locally on the machine hosting Jazero to avoid transferring embeddings to a remote machine
     * @param jazeroDir Path to Jazero repository
     * @param embeddingsFile The embeddings file
     * @param delimiter Delimiter string
     * @return Statistics about number of loaded entities and disk memory produced from loading embeddings
     */
    @Override
    public Map<String, Double> insertEmbeddings(File jazeroDir, File embeddingsFile, String delimiter)
    {
        String mountedPath = jazeroDir.getAbsolutePath() + "/" + RELATIVE_MOUNT, kgDir = "/home/" + RELATIVE_MOUNT;

        try
        {
            copyDir(embeddingsFile.getAbsolutePath(), mountedPath);
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed copying embeddings file '" + embeddingsFile + "' to mounted Jazero directory '" + mountedPath + "'");
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", MediaType.APPLICATION_JSON_VALUE);

        JsonObject json = new JsonObject();
        json.addProperty("file", kgDir + embeddingsFile.getName());
        json.addProperty("delimiter", delimiter);

        try
        {
            Communicator comm = ServiceCommunicator.init(Configuration.getSDLManagerHost(), Configuration.getSDLManagerPort(), "/embeddings");
            Response response = comm.send(json.toString(), headers);
            Map<String, Double> stats = new HashMap<>();

            if (response.getResponseCode() != 200)
            {
                throw new RuntimeException("Response code " + response.getResponseCode() + " was returned");
            }

            String responseStats = (String) response.getResponse();
            stats.put("Loaded entities", Double.valueOf(responseStats.split("Loaded ")[1].split(" entity")[0]));
            stats.put("Loaded memory", Double.valueOf(responseStats.split("(")[1].split(" mb")[0]));

            return stats;
        }

        catch (IOException e)
        {
            throw new RuntimeException("Failed inserting embeddings into Jazero: " + e.getMessage());
        }
    }
}
