package dk.aau.cs.dkwe.edao.calypso.storagelayer;

import dk.aau.cs.dkwe.edao.calypso.storagelayer.layer.Storage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StorageHandlerTest
{
    private StorageHandler handlerNative = new StorageHandler(StorageHandler.StorageType.NATIVE),
            handlerHDFS = new StorageHandler(StorageHandler.StorageType.HDFS);
    private File source1 = new File("source1.txt"),
            source2 = new File("source2.txt"),
            source3 = new File("source3.txt");

    @Before
    public void setup() throws IOException
    {
        this.source1.createNewFile();
        this.source2.createNewFile();
        this.source3.createNewFile();
    }

    @After
    public void tearDown()
    {
        this.source1.delete();
        this.source2.delete();
        this.source3.delete();
    }

    private void testInsertIntoStorage(StorageHandler handler)
    {
        handler.insert(this.source1);
        assertTrue(new File(handler.getStorageDirectory() + "/" + this.source1.getName()).exists());

        new File(handler.getStorageDirectory() + "/" + this.source1.getName()).delete();
        handler.getStorageDirectory().delete();
    }

    @Test
    public void testInsert()
    {
        testInsertIntoStorage(this.handlerNative);
        //testInsertIntoStorage(this.handlerHDFS);
    }

    private void testCountInStorage(StorageHandler handler)
    {
        handler.insert(this.source1);
        handler.insert(this.source2);
        handler.insert(this.source3);

        assertTrue(handler.getStorageDirectory().exists());
        assertEquals(3, handler.count());

        new File(handler.getStorageDirectory() + "/" + this.source1.getName()).delete();
        new File(handler.getStorageDirectory() + "/" + this.source2.getName()).delete();
        new File(handler.getStorageDirectory() + "/" + this.source3.getName()).delete();
        handler.getStorageDirectory().delete();
    }

    @Test
    public void testCount()
    {
        testCountInStorage(this.handlerNative);
        //testCountInStorage(this.handlerHDFS);
    }

    private void testIteratorFromStorage(StorageHandler handler)
    {
        handler.insert(this.source1);
        handler.insert(this.source2);
        handler.insert(this.source3);

        assertTrue(handler.getStorageDirectory().exists());

        Iterator<File> iter = handler.iterator();
        Set<File> files = new HashSet<>();

        while (iter.hasNext())
        {
            files.add(iter.next());
        }

        assertEquals(3, files.size());
        assertTrue(files.contains(new File(handler.getStorageDirectory() + "/" + this.source1.getName())));
        assertTrue(files.contains(new File(handler.getStorageDirectory() + "/" + this.source2.getName())));
        assertTrue(files.contains(new File(handler.getStorageDirectory() + "/" + this.source3.getName())));

        new File(handler.getStorageDirectory() + "/" + this.source1.getName()).delete();
        new File(handler.getStorageDirectory() + "/" + this.source2.getName()).delete();
        new File(handler.getStorageDirectory() + "/" + this.source3.getName()).delete();
        handler.getStorageDirectory().delete();
    }

    @Test
    public void testIterator()
    {
        testIteratorFromStorage(this.handlerNative);
        //testIteratorFromStorage(this.handlerHDFS);
    }
}
