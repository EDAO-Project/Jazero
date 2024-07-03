package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

public interface Indexable
{
    boolean index();
    Object getIndexable();
    String getId();
    int getPriority();
    void setPriority(int priority);
    boolean isIndexed();
}
