package dk.aau.cs.dkwe.edao.calypso.storagelayer.layer;

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

public class DiskTest
{
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

    @Test
    public void testInsert()
    {
        File dir = new File("test/");
        Disk d = new Disk(dir);
        d.insert(this.source1);
        assertTrue(new File("test/" + this.source1.getName()).exists());

        new File(dir + "/" + this.source1.getName()).delete();
        dir.delete();
    }

    @Test
    public void testCount()
    {
        File dir = new File("test/");
        Disk d = new Disk(dir);
        d.insert(this.source1);
        d.insert(this.source2);
        d.insert(this.source3);

        assertTrue(dir.exists());
        assertEquals(3, d.count());

        new File(dir + "/" + this.source1.getName()).delete();
        new File(dir + "/" + this.source2.getName()).delete();
        new File(dir + "/" + this.source3.getName()).delete();
        dir.delete();
    }

    @Test
    public void testIterator()
    {
        File dir = new File("test/");
        Disk d = new Disk(dir);
        d.insert(this.source1);
        d.insert(this.source2);
        d.insert(this.source3);

        assertTrue(dir.exists());

        Iterator<File> iter = d.iterator();
        Set<File> files = new HashSet<>();

        while (iter.hasNext())
        {
            files.add(iter.next());
        }

        assertEquals(3, files.size());
        assertTrue(files.contains(new File("test/source1.txt")));
        assertTrue(files.contains(new File("test/source2.txt")));
        assertTrue(files.contains(new File("test/source3.txt")));

        new File(dir + "/" + this.source1.getName()).delete();
        new File(dir + "/" + this.source2.getName()).delete();
        new File(dir + "/" + this.source3.getName()).delete();
        dir.delete();
    }
}
