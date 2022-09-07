package dk.aau.cs.dkwe.edao.calypso.datalake.connector.embeddings;

import dk.aau.cs.dkwe.edao.calypso.datalake.connector.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EmbeddingDBWrapper implements DBDriverBatch<List<Double>, String>, ExplainableCause, Setup
{
    private Object driver;
    private static final String IRI_FIELD = "iri";
    private static final String EMBEDDING_FIELD = "embedding";
    private static final String COLLECTION_NAME = "Embeddings";

    public EmbeddingDBWrapper(DBDriver<?, ?> db, boolean doSetup)
    {
        this.driver = db;

        if (doSetup)
            setup();
    }

    @Override
    public void setup()
    {
        if (this.driver instanceof Setup)
            ((Setup) this.driver).setup();

        DBDriver<ResultSet, String> sql = (DBDriver<ResultSet, String>) this.driver;

        if (this.driver instanceof Postgres)
        {
            if (!sql.update("CREATE TABLE IF NOT EXISTS " + COLLECTION_NAME + " (" +
                    IRI_FIELD + " VARCHAR(1000) PRIMARY KEY, " +
                    EMBEDDING_FIELD + " FLOAT[] NOT NULL);"))
                throw new RuntimeException("Setup failed: EmbeddingDBWrapper");

            if (!sql.update("CREATE INDEX hash_idx ON " + COLLECTION_NAME + " using hash (" + IRI_FIELD + ");"))
                throw new RuntimeException("Creating hash index failed: EmbeddingDBWrapper\n" + ((Postgres) sql).getError());
        }

        else
        {
            if (!sql.update("CREATE TABLE IF NOT EXISTS " + COLLECTION_NAME + " (" +
                    IRI_FIELD + " VARCHAR(1000) PRIMARY KEY, " +
                    EMBEDDING_FIELD + " VARCHAR(1000) NOT NULL);"))
                throw new RuntimeException("Setup failed: EmbeddingDBWrapper");
        }
    }

    /**
     * Query method
     * @param iri Entity IRI to find
     * @return Embeddings of given entity IRI
     */
    @Override
    public List<Double> select(String iri)
    {
        return postgresSelect(iri);
    }

    private List<Double> postgresSelect(String iri)
    {
        Postgres postgres = (Postgres) this.driver;
        ResultSet rs = postgres.select("SELECT " + EMBEDDING_FIELD +
                " FROM " + COLLECTION_NAME +
                " WHERE " + IRI_FIELD + "='" + iri + "';");

        try
        {
            if (!rs.next())
                return null;

            return Arrays.asList((Double[]) rs.getArray(1).getArray());
        }

        catch (SQLException e)
        {
            return null;
        }
    }

    /**
     * Used to insert embeddings only
     * @param query Entity IRI and embeddings seperated by space ' '
     *              Embedding vector is comma-separated
     * @return True if update is successful
     */
    @Override
    public boolean update(String query)
    {
        return postgresUpdate(query);
    }

    private boolean postgresUpdate(String query)
    {
        Postgres postgres = (Postgres) this.driver;
        String[] split = query.split(" ");

        if (split.length != 2)
            return false;

        return postgres.update("INSERT INTO " + COLLECTION_NAME + " VALUES ('"+
                split[0] + "', '{" + split[1] + "}');");
    }

    @Override
    public boolean updateSchema(String query)
    {
        throw new UnsupportedOperationException("No need to support schema changes");
    }

    @Override
    public boolean close()
    {
        return ((DBDriver<?, ?>) this.driver).close();
    }

    /**
     * Dropping of collection/table
     * @param query Name of collection or table
     * @return True if successful
     */
    @Override
    public boolean drop(String query)
    {
        throw new UnsupportedOperationException("Cannot support dropping of tables/collections");
    }

    @Override
    public boolean batchInsert(List<String> iris, List<List<Float>> vectors)
    {
        return relationalBatchInsert(iris, vectors, "'{", "}'");
    }

    private boolean relationalBatchInsert(List<String> iris, List<List<Float>> vectors, String vecStart, String vecEnd)
    {
        if (iris.size() != vectors.size())
            return false;

        DBDriver<ResultSet, String> db = (DBDriver<ResultSet, String>) this.driver;
        StringBuilder builder = new StringBuilder("INSERT INTO " + COLLECTION_NAME + " VALUES ");

        for (int i = 0; i < iris.size(); i++)
        {
            builder.append("('").append(iris.get(i).replace("'", "''")).append("', ").append(vecStart);

            for (Float f : vectors.get(i))
            {
                builder.append(f).append(", ");
            }

            builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1).append(vecEnd).append("), ");
        }

        builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1);
        return db.update(builder.append(";").toString());
    }

    @Override
    public String getError()
    {
        if (this.driver instanceof ExplainableCause)
            return ((ExplainableCause) this.driver).getError();

        return null;
    }

    @Override
    public String getStackTrace()
    {
        if (this.driver instanceof ExplainableCause)
            return ((ExplainableCause) this.driver).getStackTrace();

        return null;
    }
}
