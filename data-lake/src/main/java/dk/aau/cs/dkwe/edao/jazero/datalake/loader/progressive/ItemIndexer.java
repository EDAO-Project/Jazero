package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

public interface ItemIndexer<I>
{
    void index(String id, int row, I item);
}
