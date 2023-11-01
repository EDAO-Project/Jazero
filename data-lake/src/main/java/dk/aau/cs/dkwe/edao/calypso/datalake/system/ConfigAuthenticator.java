package dk.aau.cs.dkwe.edao.calypso.datalake.system;

// This is an insecure authenticator with no encryption of user logins
// Another class should be used for this purpose
public class ConfigAuthenticator extends Authenticator
{
    @Override
    public Auth authenticate(String username, String password)
    {
        User user = Configuration.getUserAuthenticate(username);

        if (user == null || !password.equals(user.password()))
        {
            return Auth.NOT_AUTH;
        }

        else if (user.readOnly())
        {
            return Auth.READ;
        }

        return Auth.WRITE;
    }

    @Override
    public void allow(User user)
    {
        Configuration.setUserAuthenticate(user);
    }

    @Override
    public void disallow(String username)
    {
        User foundUser = Configuration.getUserAuthenticate(username);

        if (foundUser != null)
        {
            Configuration.removeUserAuthenticate(username);
        }
    }
}
