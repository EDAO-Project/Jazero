package dk.aau.cs.dkwe.edao.calypso.cdlc;

import dk.aau.cs.dkwe.edao.calypso.cdlc.query.Query;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.calypso.storagelayer.StorageHandler;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

public interface Connector
{
    boolean isConnected();
    Iterator<Pair<File, Double>> search(int k, ScoringType scoringType, Query query);
    Map<String, Double> insert(File tablesDirectory, File calypsoDir, StorageHandler.StorageType storageType, String tableEntityPrefix, String kgEntityPrefix);
    Map<String, Double> insertEmbeddings(File calypsoDir, File embeddingsFile, String delimiter);
}
