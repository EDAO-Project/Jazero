package dk.aau.cs.dkwe.edao.jazero.web.chat.spi;

import dk.aau.cs.dkwe.edao.jazero.web.chat.Channel;

import java.util.List;
import java.util.Optional;

public interface ChannelRepository
{
    List<Channel> findAll();

    Channel save(NewChannel newChannel);

    Optional<Channel> findById(String channelId);

    boolean exists(String channelId);
}
