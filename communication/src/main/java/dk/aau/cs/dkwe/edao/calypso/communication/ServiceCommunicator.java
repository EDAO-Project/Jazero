package dk.aau.cs.dkwe.edao.calypso.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public class ServiceCommunicator implements Communicator
{
    private URL url, testUrl;

    public static ServiceCommunicator init(String hostName, String mapping) throws MalformedURLException
    {
        String m = (mapping.startsWith("/") ? "" : "/") + mapping;
        URL url = new URL("http", hostName, m);
        return new ServiceCommunicator(url);
    }

    public static ServiceCommunicator init(String hostName, int port, String mapping) throws MalformedURLException
    {
        String m = (mapping.startsWith("/") ? "" : "/") + mapping;
        URL url = new URL("http", hostName, port, m);
        return new ServiceCommunicator(url);
    }

    private ServiceCommunicator(URL url) throws MalformedURLException
    {
        this.url = url;
        this.testUrl = new URL(this.url.getProtocol(), this.url.getHost(), this.url.getPort(), "/ping");
    }

    @Override
    public boolean testConnection()
    {
        try
        {
            HttpURLConnection connection = (HttpURLConnection) this.testUrl.openConnection();
            boolean established = connection.getResponseCode() < 400 && connection.getResponseMessage().equals("Pong");
            connection.disconnect();
            return established;
        }

        catch (IOException exc)
        {
            return false;
        }
    }

    /**
     * Send POST request
     * @param content Content to be send in request body
     * @param headers Headers of POST request
     * @return Response code
     * @throws IOException
     */
    @Override
    public Object send(Object content, Map<String, String> headers) throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) this.url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);

        for (Map.Entry<String, String> entry : headers.entrySet())
        {
            connection.setRequestProperty(entry.getKey(), entry.getValue());
        }

        byte[] data = content.toString().getBytes();
        OutputStream stream = connection.getOutputStream();
        stream.write(data);

        Object response = read(connection.getInputStream());
        connection.disconnect();
        return connection.getResponseCode();
    }

    @Override
    public Object receive() throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) this.url.openConnection();
        Object response = read(connection.getInputStream());
        connection.disconnect();
        return response;
    }

    private static Object read(InputStream stream) throws IOException
    {
        int c;
        StringBuilder builder = new StringBuilder();

        while ((c = stream.read()) != -1)
        {
            builder.append((char) c);
        }

        return builder.toString();
    }
}
