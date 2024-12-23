package dk.aau.cs.dkwe.edao.connector;

import com.google.gson.*;
import dk.aau.cs.dkwe.edao.jazero.communication.Communicator;
import dk.aau.cs.dkwe.edao.jazero.communication.Response;
import dk.aau.cs.dkwe.edao.jazero.communication.ServiceCommunicator;
import dk.aau.cs.dkwe.edao.jazero.datalake.connector.service.Service;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.Result;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.User;
import dk.aau.cs.dkwe.edao.structures.Query;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataLakeService extends Service implements DataLake
{
    private final Map<String, String> headers = new HashMap<>();
    private static final int DL_PORT = 8081;
    private static final int EL_PORT = 8082;
    private static final int KG_PORT = 8083;

    public DataLakeService(String host, User user)
    {
        super(host, DL_PORT, false);
        this.headers.put("username", user.username());
        this.headers.put("password", user.password());
        this.headers.put("Content-Type", "application/json");
    }

    /**
     * Pings the Jazero services
     * Use this to check the connection
     * @return 'True' if connected
     */
    @Override
    public Response ping()
    {
        try
        {
            Communicator dlComm = ServiceCommunicator.init(getHost(), DL_PORT, "ping"),
                    elComm = ServiceCommunicator.init(getHost(), EL_PORT, "ping"),
                    kgComm = ServiceCommunicator.init(getHost(), KG_PORT, "ping");
            Response dlResponse = dlComm.receive(this.headers), elResponse = elComm.receive(this.headers),
                    kgResponse = kgComm.receive(this.headers);

            if (dlResponse.getResponseCode() >= 300 || elResponse.getResponseCode() >= 300
                    || kgResponse.getResponseCode() >= 300)
            {
                return new Response(Math.max(dlResponse.getResponseCode(),
                        Math.max(elResponse.getResponseCode(), kgResponse.getResponseCode())), "Ping failed");
            }

            Object dlOutput = dlResponse.getResponse(), elOutput = elResponse.getResponse(),
                    kgOutput = kgResponse.getResponse();

            if (dlOutput instanceof String dlStr && dlStr.equals("pong") &&
                    elOutput instanceof String elStr && elStr.equals("pong") &&
                    kgOutput instanceof String kgStr && kgStr.equals("pong"))
            {
                return new Response(200, "pong");
            }

            return new Response(400, "Ping failed");
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute table query
     * @param query Example table query
     * @param k Top-K
     * @param entitySimilarity Entity similarity metric
     * @param prefilter Whether to prefilter the search space to improve runtime and barely sacrificing any quality
     * @return Ranking of tables by semantic relevance
     */
    @Override
    public Result search(Query query, int k, TableSearch.EntitySimilarity entitySimilarity, boolean prefilter)
    {
        return search(query, k, entitySimilarity, 0, prefilter);
    }

    /**
     * Execute table query
     * @param query Example table query
     * @param k Top-K
     * @param entitySimilarity Entity similarity metric
     * @param queryWait Time to wait before querying (allows some indexing time when executing many queries at once)
     * @param prefilter Whether to prefilter the search space to improve runtime and barely sacrificing any quality
     * @return Ranking of tables by semantic relevance
     */
    @Override
    public Result search(Query query, int k, TableSearch.EntitySimilarity entitySimilarity, int queryWait, boolean prefilter)
    {
        try (InputStream queryStream = query.serialize())
        {
            Communicator comm = ServiceCommunicator.init(getHost(), DL_PORT, "search");
            byte[] queryBuffer = queryStream.readAllBytes();
            JsonObject body = new JsonObject();
            body.add("top-k", new JsonPrimitive(k));
            body.add("similarity-measure", new JsonPrimitive("EUCLIDEAN"));
            body.add("entity-similarity", new JsonPrimitive(entitySimilarity.toString()));
            body.add("single-column-per-query-entity", new JsonPrimitive("true"));
            body.add("use-max-similarity-per-column", new JsonPrimitive("true"));
            body.add("weighted-jaccard", new JsonPrimitive("true"));
            body.add("pre-filter", new JsonPrimitive(prefilter));
            body.add("query-time", new JsonPrimitive(queryWait));
            body.add("query", new JsonPrimitive(new String(queryBuffer)));
            body.add("cosine-function", new JsonPrimitive(switch (entitySimilarity) {
                case EMBEDDINGS_ABS -> "ABS_COS";
                case EMBEDDINGS_ANG -> "ANG_COS";
                case EMBEDDINGS_NORM -> "NORM_COS";
                default -> "none";
            }));

            Response response = comm.send(body.toString(), this.headers);

            if (response.getResponseCode() != 200)
            {
                throw new RuntimeException("Search request failed. Received response code " + response.getResponseCode() + ".");
            }

            JsonElement jsonResult = JsonParser.parseString((String) response.getResponse());
            double runtime = jsonResult.getAsJsonObject().get("runtime").getAsDouble(),
                    reduction = jsonResult.getAsJsonObject().get("reduction").getAsDouble();
            JsonArray jsonResultArray = jsonResult.getAsJsonObject().getAsJsonArray("scores");
            List<Pair<File, Double>> results = new ArrayList<>(k);

            for (JsonElement element : jsonResultArray)
            {
                String tableId = element.getAsJsonObject().get("table ID").getAsString();
                File tableFile = new File(tableId);
                double score = element.getAsJsonObject().get("score").getAsDouble();
                results.add(new Pair<>(tableFile, score));
            }

            return new Result(k, results, runtime, reduction, new HashMap<>());
        }

        catch (IOException e)
        {
            throw new RuntimeException("Search request error", e);
        }
    }

    /**
     * Clears the indexes in the Jazero data lake
     * @return 'True' if successful. Appropriate exceptions are thrown on error.
     */
    @Override
    public Response clear()
    {
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), DL_PORT, "clear");
            return comm.receive(this.headers);
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Clears embeddings from the database but not from the indexes.
     * Hence, you can still perform table search using embeddings after calling this.
     * @return 'True' if successful. Appropriate exceptions are thrown on error.
     */
    @Override
    public Response clearEmbeddings()
    {
        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), DL_PORT, "clear-embeddings");
            return comm.receive(this.headers);
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Removes a table from the data lake
     * @param tableId Identifier of table to be removed
     * @return 'True' if the table was removed successfully
     */
    @Override
    public Response removeTable(String tableId)
    {
        JsonObject body = new JsonObject();
        body.add("table", new JsonPrimitive(tableId));

        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), DL_PORT, "remove-table");
            return comm.send(body.toString(), this.headers);
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a new user with given privileges
     * @param newUser New user to be added with certain privileges
     * @return 'True' if successful. Appropriate exceptions are thrown on error.
     */
    @Override
    public Response addUser(User newUser)
    {
        JsonObject body = new JsonObject();
        body.add("new-username", new JsonPrimitive(newUser.username()));
        body.add("new-password", new JsonPrimitive(newUser.password()));

        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), DL_PORT, "add-user");
            return comm.send(body.toString(), this.headers);
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Remove a user
     * @param removedUser User to remove from Jazero
     * @return 'True' if successful. Appropriate exceptions are thrown on error.
     */
    @Override
    public Response removeUser(User removedUser)
    {
        JsonObject body = new JsonObject();
        body.add("old-username", new JsonPrimitive(removedUser.username()));

        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), DL_PORT, "remove-user");
            return comm.send(body.toString(), this.headers);
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of tables containing an entity
     * Note that the results is orthogonal to the accuracy of the entity linker
     * @param uri URI of entity to count
     * @return Number of data lake tables containing an entity
     */
    @Override
    public Response count(String uri)
    {
        this.headers.put("entity", uri);

        try
        {
            Communicator comm = ServiceCommunicator.init(getHost(), DL_PORT, "count");
            return comm.receive(this.headers);
        }

        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        finally
        {
            this.headers.remove("entity");
        }
    }
}
