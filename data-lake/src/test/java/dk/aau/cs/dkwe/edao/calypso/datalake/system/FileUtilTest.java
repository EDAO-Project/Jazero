package dk.aau.cs.dkwe.edao.calypso.datalake.system;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.springframework.test.util.AssertionErrors.assertEquals;
import static org.springframework.test.util.AssertionErrors.assertTrue;

public class FileUtilTest
{
    private File testDir1 = new File("test/test1"),
                    testDir2 = new File("test/test2"),
                    testFile = new File("test/test1/file1.txt");

    @BeforeEach
    public void setup() throws IOException
    {
        this.testDir1.mkdirs();
        this.testDir2.mkdirs();
        this.testFile.createNewFile();
    }

    @AfterEach
    public  void tearDown()
    {
        this.testFile.delete();
        this.testDir1.delete();
        this.testDir2.delete();
        new File("test/").delete();
    }

    @Test
    public void testMoveFile()
    {
        assertEquals("Move operation of file did not return exit code 0",
                0, FileUtil.move(this.testFile, this.testDir2));
        assertTrue("File was not moved",
                new File(this.testDir2.toString() + "/" + this.testFile.getName()).exists());
    }

    @Test
    public void testMoveDir()
    {
        assertEquals("Move operation of directory did not return exit code 0",
                0, FileUtil.move(this.testDir1, this.testDir2));
        assertTrue("Directory was not moved",
                new File(this.testDir2.toString() + "/" + this.testDir1.getName()).isDirectory());
    }

    @Test
    public void testCopyFile()
    {
        assertEquals("Copy operation of file did not return exit code 0",
                0, FileUtil.copy(this.testFile, this.testDir2));
        assertTrue("File was not copied",
                new File(this.testDir2.toString() + "/" + this.testFile.getName()).exists());
    }

    @Test
    public void testCopyDir()
    {
        assertEquals("Copy operation of directory did not return exit code 0",
                0, FileUtil.copy(this.testDir1, this.testDir2));
        assertTrue("Directory was not copied",
                new File(this.testDir2.toString() + "/" + this.testDir1).exists());
    }
}
