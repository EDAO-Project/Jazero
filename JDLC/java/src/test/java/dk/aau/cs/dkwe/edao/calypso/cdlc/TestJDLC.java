package dk.aau.cs.dkwe.edao.calypso.cdlc;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class TestJDLC
{
    @Test
    public void testIsConnected()
    {
        JDLC connector = new JDLC();
        assertFalse(connector.isConnected());
    }
}
