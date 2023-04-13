package eu.ibagroup.vf.strategy;

import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.conversations.ConversationsListResponse;
import com.slack.api.methods.response.users.UsersListResponse;
import com.slack.api.model.Conversation;
import com.slack.api.model.User;
import com.slack.api.model.block.LayoutBlock;
import com.slack.api.model.block.SectionBlock;
import com.slack.api.model.block.composition.MarkdownTextObject;
import eu.ibagroup.vf.model.Notification;
import eu.ibagroup.vf.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.StringUtils;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static eu.ibagroup.vf.model.EnvParam.SLACK_TOKEN;
import static eu.ibagroup.vf.util.CommonUtils.INPUT_DATETIME_FORMAT;
import static eu.ibagroup.vf.util.CommonUtils.OUTPUT_DATETIME_FORMAT;

/**
 * Class, contains methods for sending notifications to Slack.
 */
@Slf4j
public class SlackNotificationService implements SendingStrategy {

    private static final String CHANNEL_INDICATOR = "#";
    private static final String CHANNELS_DELIMITER = ",";
    private static final String CHANNELS_JOINING = ", ";
    private static final String USER_PLACEHOLDER = "<@%s>";
    private final MethodsClient client;
    private final UsersListResponse allUsers;
    private final String template;

    /**
     * Required args constructor. Initialize template and all users list.
     */
    public SlackNotificationService(String template) {
        this.template = template;
        try {
            this.client = Slack.getInstance().methods();
            allUsers = client.usersList(r -> r.token(CommonUtils.getEnvProperty(SLACK_TOKEN)));
        } catch (Exception e) {
            LOGGER.error("An error has occurred during connection to Slack: {}", e.getMessage());
            throw new IllegalStateException("Unable to connect to Slack", e);
        }
    }

    /**
     * Find channels conversations IDs using the conversations name.
     *
     * @param channels are channels names.
     * @return an ID of the conversations.
     */
    public Map<String, Optional<String>> findChannelsConversations(List<String> channels) {
        Map<String, Optional<String>> channelsMap = new HashMap<>();
        ConversationsListResponse result;
        try {
            result = client.conversationsList(r -> r.token(CommonUtils.getEnvProperty(SLACK_TOKEN)));
        } catch (Exception e) {
            LOGGER.error("An error has occurred during getting channels from Slack: {}", e.getMessage());
            throw new IllegalStateException("Unable to get all channels", e);
        }
        for (String channelName : channels) {
            for (Conversation channel : result.getChannels()) {
                if (channel.getName().equals(channelName)) {
                    channelsMap.put(channelName, Optional.of(channel.getId()));
                    break;
                }
            }
            channelsMap.computeIfAbsent(channelName, key -> {
                LOGGER.warn("The channel with '{}' name has not been found in Slack", channelName);
                return Optional.empty();
            });
        }
        return channelsMap;
    }

    /**
     * Find users conversations IDs using the usernames.
     *
     * @param usernames is a list of usernames.
     * @return IDs of the conversations.
     */
    public Map<String, Optional<String>> findUsersConversations(List<String> usernames) {
        Map<String, Optional<String>> users = new HashMap<>();
        for (String username : usernames) {
            for (User user : allUsers.getMembers()) {
                if (user.getName().equals(username)) {
                    users.put(username, Optional.of(user.getId()));
                    break;
                }
            }
            users.computeIfAbsent(username, key -> {
                LOGGER.warn("The user with '{}' nickname has not been found in Slack", username);
                return Optional.empty();
            });
        }
        return users;
    }

    /**
     * Method, sends a message to the specified channel.
     *
     * @param id   is channel's ID.
     * @param text is the text of the message
     * @return the ID of sent message.
     */
    public Optional<String> publishMessage(String id, String text) {
        List<LayoutBlock> message = List.of(SectionBlock
                .builder()
                .text(MarkdownTextObject
                        .builder()
                        .text(text.replace("\\n", "\n"))
                        .build())
                .build()
        );
        try {
            ChatPostMessageResponse result = client.chatPostMessage(r -> r
                    .token(CommonUtils.getEnvProperty(SLACK_TOKEN))
                    .channel(id)
                    .mrkdwn(true)
                    .blocks(message)
            );
            return Optional.of(result.getTs());
        } catch (Exception e) {
            LOGGER.error("An error has occurred during sending a message to {} channel: {}", id, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Secondary method for parsing channels string to list and finding for each channel ID.
     *
     * @param findFunction function, find channels IDs.
     * @param notification is notification object.
     * @return a list of all channels' IDs.
     */
    private Collection<Optional<String>> parseAndFindChannels(
            Function<List<String>, Map<String, Optional<String>>> findFunction,
            Notification notification) {
        List<String> channels = Arrays.asList(notification.getSendTo()
                .replace(CHANNEL_INDICATOR, "")
                .split(CHANNELS_DELIMITER));
        return findFunction.apply(channels).values();
    }

    @Override
    public Optional<String> constructNotificationBody(String... params) {
        try {
            String templateContent = IOUtils.toString(CommonUtils.getTemplate(template), StandardCharsets.UTF_8);
            LocalDateTime startedAt = LocalDateTime.parse(params[6], DateTimeFormatter.ofPattern(INPUT_DATETIME_FORMAT));
            LocalDateTime finishedAt = startedAt.plusSeconds(Double.valueOf(params[7]).longValue());
            String welcomeLabel = "";
            if (StringUtils.isNotEmpty(params[0])) {
                List<String> usersList = Arrays.asList(params[0].split(CHANNELS_DELIMITER));
                String users = findUsersConversations(usersList)
                        .entrySet()
                        .stream()
                        .map(user -> user.getValue().isPresent()
                                ? String.format(USER_PLACEHOLDER, user.getValue().get())
                                : user.getKey())
                        .collect(Collectors.joining(CHANNELS_JOINING));
                welcomeLabel = String.format(">*Hello, %s!*%n>%n", users);
            }
            return Optional.of(String.format(templateContent, welcomeLabel, params[1], params[2], params[3], params[4],
                    getEmojiForStatus(params[5]), params[5],
                    finishedAt.format(DateTimeFormatter.ofPattern(OUTPUT_DATETIME_FORMAT, Locale.ENGLISH)),
                    params[1], finishedAt.getYear()));
        } catch (Exception e) {
            LOGGER.error("An error has occurred during template parsing: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Secondary method for inserting a necessary emoji for pipeline status.
     *
     * @param status pipeline work status (param 5).
     * @return slack emoji-code.
     */
    private String getEmojiForStatus(String status) {
        switch (status) {
            case "Succeeded":
                return ":large_green_circle:";
            case "Suspended":
            case "Terminated":
            case "Stopped":
                return ":large_orange_circle:";
            case "Error":
            case "Failed":
                return ":red_circle:";
            default:
                return ":large_blue_circle:";
        }
    }

    @Override
    public boolean sendNotification(Notification notification) {
        List<Optional<String>> channelsIds = new ArrayList<>();
        if (notification.getSendTo().startsWith(CHANNEL_INDICATOR)) {
            channelsIds.addAll(parseAndFindChannels(this::findChannelsConversations, notification));
        } else {
            channelsIds.addAll(parseAndFindChannels(this::findUsersConversations, notification));
        }
        List<String> result = new ArrayList<>();
        if (!channelsIds.isEmpty()) {
            channelsIds.stream()
                    .filter(Optional::isPresent)
                    .forEach(channelId -> {
                        Optional<String> messageId = publishMessage(channelId.get(), notification.getText());
                        messageId.ifPresent(result::add);
                    });
        }
        return result.size() == channelsIds.size() && !channelsIds.isEmpty();
    }
}
