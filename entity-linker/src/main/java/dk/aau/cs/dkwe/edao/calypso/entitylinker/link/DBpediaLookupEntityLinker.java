package dk.aau.cs.dkwe.edao.calypso.entitylinker.link;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import dk.aau.cs.dkwe.edao.calypso.communication.Communicator;
import dk.aau.cs.dkwe.edao.calypso.communication.ServiceCommunicator;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Uses DBpedia lookup service to find DBpedia entities and check for existence according to the EKG Manager
 */
public class DBpediaLookupEntityLinker implements EntityLink<String, String>
{
    public static DBpediaLookupEntityLinker make()
    {
        return new DBpediaLookupEntityLinker();
    }

    private DBpediaLookupEntityLinker() {}

    /**
     * Performs a lookup query using the DBpedia lookup service and check the EKG Manager for existence of the entity
     * @param key Entity to perform lookup with
     * @return Linked entity
     */
    @Override
    public String link(String key)
    {
        try
        {
            String mapping = "/api/search/KeywordSearch?&QueryString=" + key.replace(" ", "%20");
            Communicator comm = ServiceCommunicator.init("lookup.dbpedia.org", mapping, true);
            Map<String, String> headers = new HashMap<>();
            headers.put("Accept", "application/json");

            DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = docBuilder.parse((String) comm.receive(headers));
            NodeList list = doc.getElementsByTagName("ArrayOfResults");

            if (list.getLength() == 0)
            {
                return null;
            }

            NodeList resultNodes = list.item(0).getChildNodes();

            for (int i = 0; i < resultNodes.getLength(); i++)
            {
                if (resultNodes.item(i).getNodeName().equals("URI"))
                {
                    return resultNodes.item(i).getNodeValue();
                }
            }

            return null;
        }

        catch (ParserConfigurationException e)
        {
            throw new RuntimeException("Configuration error in XML parser: " + e.getMessage());
        }

        catch (SAXException e)
        {
            throw new RuntimeException("XML parsing error: " + e.getMessage());
        }

        catch (MalformedURLException e)
        {
            throw new RuntimeException("Error with Wikipedia URL: " + e.getMessage());
        }

        catch (IOException e)
        {
            throw new RuntimeException("Error when reading Wikipedia response: " + e.getMessage());
        }
    }
}
