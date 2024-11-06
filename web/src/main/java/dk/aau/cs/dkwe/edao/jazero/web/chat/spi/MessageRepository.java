package dk.aau.cs.dkwe.edao.jazero.web.chat.spi;

import dk.aau.cs.dkwe.edao.jazero.web.chat.Message;
import jakarta.annotation.Nullable;

import java.util.List;

public interface MessageRepository
{
    List<Message> findLatest(String channelId, int fetchMax, @Nullable String lastSeenMessageId);

    default List<Message> findLatest(String channelId, int fetchMax) {
        return findLatest(channelId, fetchMax, null);
    }

    Message save(NewMessage newMessage);
}
