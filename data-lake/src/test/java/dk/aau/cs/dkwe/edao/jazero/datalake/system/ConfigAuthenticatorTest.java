package dk.aau.cs.dkwe.edao.jazero.datalake.system;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigAuthenticatorTest
{
    @BeforeEach
    public void setup()
    {
        Configuration.debug();
    }

    @AfterEach
    public void clear() throws IOException
    {
        Files.delete(new File(".config.conf").toPath());
    }

    @Test
    public void testAuthenticatorReadOnly()
    {
        User u = new User("username", "password", true);
        Authenticator authenticator = Configuration.initAuthenticator();
        authenticator.allow(u);

        Authenticator.Auth auth = authenticator.authenticate("username", "password");
        assertEquals(Authenticator.Auth.READ, auth);
    }

    @Test
    public void testAuthenticatorWrite()
    {
        User u = new User("username", "password", false);
        Authenticator authenticator = Configuration.initAuthenticator();
        authenticator.allow(u);

        Authenticator.Auth auth = authenticator.authenticate("username", "password");
        assertEquals(Authenticator.Auth.WRITE, auth);
    }

    @Test
    public void testAuthenticatorFail()
    {
        User u = new User("username1", "password1", false);
        Authenticator authenticator = Configuration.initAuthenticator();
        authenticator.allow(u);

        Authenticator.Auth auth = authenticator.authenticate("username2", "password2");
        assertEquals(Authenticator.Auth.NOT_AUTH, auth);
    }

    @Test
    public void testAuthenticatorMany()
    {
        User u1 = new User("username3", "password3", false),
                u2 = new User("username4", "password4", true),
                u3 = new User("username5", "password5", false);
        Configuration.initAuthenticator().allow(u1);
        Configuration.initAuthenticator().allow(u2);
        Configuration.initAuthenticator().allow(u3);

        Authenticator.Auth auth1 = Configuration.initAuthenticator().authenticate("username3", "password3"),
                auth2 = Configuration.initAuthenticator().authenticate("username4", "password4"),
                auth3 = Configuration.initAuthenticator().authenticate("username5", "password5");
        assertEquals(Authenticator.Auth.WRITE, auth1);
        assertEquals(Authenticator.Auth.READ, auth2);
        assertEquals(Authenticator.Auth.WRITE, auth3);
    }

    @Test
    public void testRemoveAuthentication()
    {
        User u1 = new User("username6", "password6", false),
                u2 = new User("username7", "password7", true);
        Configuration.initAuthenticator().allow(u1);
        Configuration.initAuthenticator().allow(u2);

        Authenticator.Auth auth1 = Configuration.initAuthenticator().authenticate("username6", "password6"),
                auth2 = Configuration.initAuthenticator().authenticate("username7", "password7");
        assertEquals(Authenticator.Auth.WRITE, auth1);
        assertEquals(Authenticator.Auth.READ, auth2);
        Configuration.initAuthenticator().disallow(u1.username());

        Authenticator.Auth authRemoved = Configuration.initAuthenticator().authenticate("username6", "password6");
        assertEquals(Authenticator.Auth.NOT_AUTH, authRemoved);
    }
}
