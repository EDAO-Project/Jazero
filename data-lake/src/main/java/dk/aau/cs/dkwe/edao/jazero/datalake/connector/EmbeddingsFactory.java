package dk.aau.cs.dkwe.edao.jazero.datalake.connector;

import dk.aau.cs.dkwe.edao.jazero.datalake.connector.embeddings.EmbeddingDBWrapper;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.Configuration;

import java.util.List;

public class EmbeddingsFactory
{
    public static EmbeddingDBWrapper wrap(DBDriver<?, ?> driver, boolean doSetup)
    {
        return new EmbeddingDBWrapper(driver, doSetup);
    }

    public static DBDriverBatch<List<Double>, String> fromConfig(boolean doSetup)
    {
        return wrap(Postgres.init(Configuration.getDBHost(), Configuration.getDBPort(), Configuration.getDBName(),
                Configuration.getDBUsername(), Configuration.getDBPassword()), doSetup);
    }
}
