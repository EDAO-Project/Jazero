package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.vaadin.flow.component.AttachEvent;
import com.vaadin.flow.component.messages.MessageInput;
import com.vaadin.flow.component.messages.MessageList;
import com.vaadin.flow.component.messages.MessageListItem;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.router.BeforeEvent;
import com.vaadin.flow.router.HasDynamicTitle;
import com.vaadin.flow.router.HasUrlParameter;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.theme.lumo.LumoUtility;
import dk.aau.cs.dkwe.edao.jazero.web.chat.ChatService;
import dk.aau.cs.dkwe.edao.jazero.web.chat.Message;
import dk.aau.cs.dkwe.edao.jazero.web.util.LimitedSortedAppendOnlyList;
import layout.MainLayout;
import reactor.core.Disposable;

import java.util.Comparator;
import java.util.List;

@Route(value = "channel", layout = MainLayout.class)
public class ChannelView extends VerticalLayout implements HasUrlParameter<String>, HasDynamicTitle
{
    private final ChatService chatService;
    private final MessageList messageList;
    private final LimitedSortedAppendOnlyList<Message> receivedMessages;
    private String channelId;
    private final String currentUsername;
    private static final int HISTORY_SIZE = 20;

    public ChannelView(ChatService chatService)
    {
        this.currentUsername = "John Doe";
        this.chatService = chatService;
        this.receivedMessages = new LimitedSortedAppendOnlyList<>(HISTORY_SIZE, Comparator.comparing(Message::sequenceNumber));
        setSizeFull();

        messageList = new MessageList();
        this.messageList.setSizeFull();
        this.messageList.addClassNames(LumoUtility.Border.ALL);
        add(this.messageList);

        var messageInput = new MessageInput(event -> sendMessage(event.getValue()));
        messageInput.setWidthFull();
        add(messageInput);
    }

    @Override
    public void setParameter(BeforeEvent event, String channelId)
    {
        if (this.chatService.channel(channelId).isEmpty())
        {
            event.forwardTo(LobbyView.class);
        }

        this.channelId = channelId;
    }

    private void sendMessage(String message)
    {
        if (!message.isBlank())
        {
            this.chatService.postMessage(this.channelId, message);
        }
    }

    private MessageListItem createMessageListItem(Message message)
    {
        var item = new MessageListItem(message.message(), message.timestamp(), message.author());
        item.addClassNames(LumoUtility.Margin.SMALL, LumoUtility.BorderRadius.MEDIUM);

        if (message.author().equals(this.currentUsername))
        {
            item.addClassNames(LumoUtility.Background.CONTRAST_5);
        }

        return item;
    }

    private void receiveMessages(List<Message> incoming)
    {
        getUI().ifPresent(ui -> ui.access(() -> {
            this.receivedMessages.addAll(incoming);
            this.messageList.setItems(this.receivedMessages.stream().map(this::createMessageListItem).toList());
        }));
    }

    private Disposable subscribe()
    {
        var subscription = this.chatService.liveMessages(this.channelId).subscribe(this::receiveMessages);
        var lastSeenMessageId = this.receivedMessages.getLast().map(Message::messageId).orElse(null);
        receiveMessages(this.chatService.messageHistory(this.channelId, HISTORY_SIZE, lastSeenMessageId));

        return subscription;
    }

    @Override
    protected void onAttach(AttachEvent attachEvent)
    {
        var subscription = subscribe();
        addDetachListener(event -> subscription.dispose());
    }

    @Override
    public String getPageTitle()
    {
        return this.channelId;
    }
}
