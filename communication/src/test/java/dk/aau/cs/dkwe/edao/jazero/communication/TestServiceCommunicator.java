package dk.aau.cs.dkwe.edao.jazero.communication;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class TestServiceCommunicator
{
    @Test
    public void testReceive() throws IOException
    {
        ServiceCommunicator communicator = ServiceCommunicator.init("ip.jsontest.com", "", false);
        String response = (String) communicator.receive();
        assertTrue(response.startsWith("{"));
    }

    /**
     * Check to see sent content at https://webhook.site/#!/df1f1234-cb3a-4bbd-8ff9-992920340013/6964eda0-124c-4be5-8fdc-9dba29261afc/1
     */
    @Test
    public void testSend() throws IOException
    {
        ServiceCommunicator communicator = ServiceCommunicator.init("webhook.site", "/2174bbdc-20a4-438b-b9f3-1cc1eac923e9", false);
        communicator.send("Hello, World", new HashMap<>());
        String response = (String) communicator.receive();
        assertTrue(response.isEmpty());
    }
}
