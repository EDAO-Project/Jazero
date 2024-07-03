package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import java.util.Collection;
import java.util.Iterator;

public interface Scheduler extends Iterator<Indexable>
{
    void addIndexTables(Collection<Indexable> indexTables);
    void addIndexTable(Indexable indexTable);
}
