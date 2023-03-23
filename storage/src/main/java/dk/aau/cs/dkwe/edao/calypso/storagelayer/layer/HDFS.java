package dk.aau.cs.dkwe.edao.calypso.storagelayer.layer;

import java.io.File;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

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

    @Override
    public Set<File> elements()
    {
        return null;
    }

    @Override
    public Set<File> elements(Predicate<File> predicate)
    {
        return null;
    }

    @Override
    public boolean clear()
    {
        return true;
    }
}
