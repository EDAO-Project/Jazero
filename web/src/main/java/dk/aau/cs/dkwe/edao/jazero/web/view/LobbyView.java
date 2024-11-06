package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.vaadin.flow.component.AttachEvent;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.Text;
import com.vaadin.flow.component.avatar.Avatar;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.component.virtuallist.VirtualList;
import com.vaadin.flow.data.renderer.ComponentRenderer;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.router.RouterLink;
import com.vaadin.flow.theme.lumo.LumoUtility;
import dk.aau.cs.dkwe.edao.jazero.web.chat.Channel;
import dk.aau.cs.dkwe.edao.jazero.web.chat.ChatService;
import layout.MainLayout;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Locale;

@Route(value = "lobby", layout = MainLayout.class)
@PageTitle("Lobby")
public class LobbyView extends VerticalLayout
{
    private final ChatService chatService;
    private final VirtualList<Channel> channels;
    private final TextField channelNameField;
    private final Button addChannelButton;

    public LobbyView(ChatService chatService)
    {
        this.chatService = chatService;
        setSizeFull();

        this.channels = new VirtualList<>();
        this.channels.addClassNames(LumoUtility.Border.ALL, LumoUtility.Padding.SMALL, "channel-list");
        this.channels.setRenderer(new ComponentRenderer<>(this::createChannelComponent));
        add(channels);
        expand(channels);

        this.channelNameField = new TextField();
        this.channelNameField.setPlaceholder("New channel name");

        this.addChannelButton = new Button("Add channel", event -> addChannel());
        this.addChannelButton.setDisableOnClick(true);

        var toolbar = new HorizontalLayout(this.channelNameField, this.addChannelButton);
        toolbar.setWidthFull();
        toolbar.expand(this.channelNameField);
        add(toolbar);
    }

    private void refreshChannels()
    {
        this.channels.setItems(this.chatService.channels());
    }

    @Override
    protected void onAttach(AttachEvent attachEvent)
    {
        refreshChannels();
    }

    private void addChannel()
    {
        try
        {
            var nameOfNewChannel = this.channelNameField.getValue();

            if (!nameOfNewChannel.isBlank())
            {
                this.chatService.createChannel(nameOfNewChannel);
                this.channelNameField.clear();
                refreshChannels();
            }
        }

        finally
        {
            addChannelButton.setEnabled(true);
        }
    }

    private Component createChannelComponent(Channel channel)
    {
        var channelComponent = new Div();
        channelComponent.addClassNames("channel");

        var avatar = new Avatar(channel.name());
        avatar.setColorIndex(Math.abs(channel.id().hashCode() % 7));
        channelComponent.add(avatar);

        var contentDiv = new Div();
        contentDiv.addClassNames("content");
        channelComponent.add(contentDiv);

        var channelName = new Div();
        channelName.addClassNames("name");
        contentDiv.add(channelName);

        var channelLink = new RouterLink(channel.name(), ChannelView.class, channel.id());
        channelName.add(channelLink);

        if (channel.lastMessage() != null)
        {
            var lastMessageTimestamp = new Span(formatInstant(channel.lastMessage().timestamp(), getLocale()));
            lastMessageTimestamp.addClassNames("last-message-timestamp");
            channelName.add(lastMessageTimestamp);
        }

        var lastMessage = new Span();
        lastMessage.addClassNames("last-message");
        contentDiv.add(lastMessage);

        if (channel.lastMessage() != null)
        {
            var author = new Span(channel.lastMessage().author());
            author.addClassNames("author");
            lastMessage.add(author, new Text(": " + truncateMessage(channel.lastMessage().message())));
        }

        else
        {
            lastMessage.setText("No messages yet");
        }

        return channelComponent;
    }

    private String formatInstant(Instant instant, Locale locale)
    {
        return DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM)
                .withLocale(locale)
                .format(ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()));
    }

    private String truncateMessage(String msg)
    {
        return msg.length() > 50 ? msg.substring(0, 50) + "..." : msg;
    }
}
