package dk.aau.cs.dkwe.edao.calypso.cdlc;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class TestCDLC
{
    @Test
    public void testIsConnected()
    {
        CDLC connector = new CDLC();
        assertFalse(connector.isConnected());
    }
}
