package dk.aau.cs.dkwe.edao.jazero.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class ServiceCommunicator implements Communicator
{
    private final URL url, testUrl;

    public static ServiceCommunicator init(String hostName, String mapping, boolean secure) throws MalformedURLException
    {
        String m = (mapping.startsWith("/") ? "" : "/") + mapping;
        URL url = new URL("http" + (secure ? "s" : ""), hostName, m);
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

    /**
     * Test connection to Jazero service
     * @return True if connection has been established and can get a response from a GET request to service
     */
    @Override
    public boolean testConnection()
    {
        try
        {
            HttpURLConnection connection = (HttpURLConnection) this.testUrl.openConnection();
            connection.connect();
            boolean established = connection.getResponseCode() < 400 && "pong".equals(read(connection.getInputStream()));
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
     * @throws IOException when an error is encountered
     */
    @Override
    public synchronized Response send(Object content, Map<String, String> headers) throws IOException
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
        return new Response(connection.getResponseCode(), response);
    }

    @Override
    public synchronized Response receive() throws IOException
    {
        return receive(new HashMap<>());
    }

    @Override
    public synchronized Response receive(Map<String, String> headers) throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) this.url.openConnection();

        for (Map.Entry<String, String> header : headers.entrySet())
        {
            connection.setRequestProperty(header.getKey(), header.getValue());
        }

        Object response = read(connection.getInputStream());
        int responseCode = connection.getResponseCode();
        connection.disconnect();

        return new Response(responseCode, response);
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
