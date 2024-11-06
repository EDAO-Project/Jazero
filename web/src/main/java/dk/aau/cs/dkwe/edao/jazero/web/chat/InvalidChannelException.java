package dk.aau.cs.dkwe.edao.jazero.web.chat;

public class InvalidChannelException extends IllegalArgumentException
{
    public InvalidChannelException()
    {
        super("The specified channel does not exist");
    }
}
