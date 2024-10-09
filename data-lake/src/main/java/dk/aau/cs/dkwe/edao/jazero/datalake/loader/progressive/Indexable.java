package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

public interface Indexable
{
    Object index();
    Table<String> getIndexable();
    String getId();
    double getPriority();
    void setPriority(double priority);
    boolean isIndexed();
}
