package dk.aau.cs.dkwe.edao.calypso.storagelayer.layer;

import java.io.File;
import java.util.Iterator;

public class HDFS implements Storage<File>
{
    @Override
    public boolean insert(File element)
    {
        return false;
    }

    @Override
    public int count()
    {
        return 0;
    }

    @Override
    public Iterator<File> iterator()
    {
        return null;
    }
}
