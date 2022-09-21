package dk.aau.cs.dkwe.edao.calypso.datalake.system;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConfigurationTest
{
    @AfterEach
    public void reset()
    {
        (new File(".config.conf")).delete();
    }

    @Test
    public void testReadNotExists()
    {
        assertNull(Configuration.getDBHost());
    }

    @Test
    public void testDBHost()
    {
        Configuration.setDBHost("host");
        assertEquals("host", Configuration.getDBHost());
    }

    @Test
    public void testDBPort()
    {
        Configuration.setDBPort(1234);
        assertEquals(1234, Configuration.getDBPort());
    }

    @Test
    public void testDBUsername()
    {
        Configuration.setDBUsername("username");
        assertEquals("username", Configuration.getDBUsername());
    }

    @Test
    public void testDBPassword()
    {
        Configuration.setDBPassword("password");
        assertEquals("password", Configuration.getDBPassword());
    }

    @Test
    public void testDBName()
    {
        Configuration.setDBName("name");
        assertEquals("name", Configuration.getDBName());
    }

    @Test
    public void testDBPath()
    {
        Configuration.setDBPath("path");
        assertEquals("path", Configuration.getDBPath());
    }
}
