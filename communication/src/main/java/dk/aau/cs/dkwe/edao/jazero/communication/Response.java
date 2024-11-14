package dk.aau.cs.dkwe.edao.jazero.communication;

public class Response
{
    private final int responseCode;
    private final Object content;

    public Response(int responseCode, Object response)
    {
        this.responseCode = responseCode;
        this.content = response;
    }

    public int getResponseCode()
    {
        return this.responseCode;
    }

    public Object getResponse()
    {
        return this.content;
    }
}
