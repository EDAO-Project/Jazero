package dk.aau.cs.dkwe.edao.calypso.storagelayer;

import dk.aau.cs.dkwe.edao.calypso.storagelayer.layer.Disk;
import dk.aau.cs.dkwe.edao.calypso.storagelayer.layer.HDFS;
import dk.aau.cs.dkwe.edao.calypso.storagelayer.layer.Storage;

import java.io.File;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

public class StorageHandler implements Storage<File>
{
    private static final File STORAGE_DIR = new File("/srv/storage");

    static
    {
        if (!STORAGE_DIR.exists() || !STORAGE_DIR.isDirectory())
        {
            STORAGE_DIR.mkdirs();
        }
    }

    public enum StorageType
    {
        NATIVE,
        HDFS
    }

    private StorageType type;
    private Storage<File> storage;

    public StorageHandler(StorageType storageType)
    {
        this.type = storageType;
        this.storage = this.type == StorageType.NATIVE ? new Disk(STORAGE_DIR) : new HDFS();
    }

    public StorageType getStorageType()
    {
        return this.type;
    }

    public File getStorageDirectory()
    {
        return STORAGE_DIR;
    }

    @Override
    public boolean insert(File file)
    {
        return this.storage.insert(file);
    }

    @Override
    public int count()
    {
        return this.storage.count();
    }

    // TODO: Maybe we should implement our own iterator that only stores the file names without the path to the file
    // TODO: When calling .next(), we prepend the path
    // TODO: This will work as we require all files to be in the same directory
    @Override
    public Iterator<File> iterator()
    {
        return this.storage.iterator();
    }

    @Override
    public Set<File> elements()
    {
        return this.storage.elements();
    }

    @Override
    public Set<File> elements(Predicate<File> predicate)
    {
        return this.storage.elements(predicate);
    }
}
