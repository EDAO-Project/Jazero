package java.dk.aau.cs.dkwe.edao.calypso.datalake.system;

public abstract class Authenticator
{
    public enum Auth
    {
        READ, WRITE, NOT_AUTH
    }

    public abstract Auth authenticate(String username, String password);
    public abstract void allow(User user);
}
