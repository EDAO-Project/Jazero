package dk.aau.cs.dkwe.edao.calypso.communication;

public class Response
{
    private int responseCode;
    private Object content;

    Response(int responseCode, Object response)
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
