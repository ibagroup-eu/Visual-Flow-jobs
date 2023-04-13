package eu.ibagroup.vf;

import com.slack.api.RequestConfigurator;
import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.conversations.ConversationsListResponse;
import com.slack.api.methods.response.users.UsersListResponse;
import com.slack.api.model.Conversation;
import com.slack.api.model.User;
import eu.ibagroup.vf.model.Notification;
import eu.ibagroup.vf.strategy.SlackNotificationService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static eu.ibagroup.vf.model.EnvParam.SLACK_TOKEN;
import static eu.ibagroup.vf.util.TestParams.DURATION;
import static eu.ibagroup.vf.util.TestParams.FIXED_TIME;
import static eu.ibagroup.vf.util.TestParams.PIPELINE_NAME;
import static eu.ibagroup.vf.util.TestParams.PIP_DESIGNER;
import static eu.ibagroup.vf.util.TestParams.PROJECT_NAME;
import static eu.ibagroup.vf.util.TestParams.PROJECT_URL;
import static eu.ibagroup.vf.util.TestParams.SEND_TO_SLACK;
import static eu.ibagroup.vf.util.TestParams.STATUS;
import static eu.ibagroup.vf.util.TestParams.SUBJECT;
import static eu.ibagroup.vf.util.TestParams.USERNAMES_SLACK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class SlackNotificationServiceTest {

    private static final String NOTIFICATION_BODY = ">*Hello, user1, <@2Uid>, user3, <@4Uid>!*" +
            System.lineSeparator() + ">" + System.lineSeparator() +
            ">The pipeline *<http://myhost.rs/vf/ui/pipelines/projectName/pipID|pipName>* " +
            "of project *<http://myhost.rs/vf/ui/projectName/overview|projectName>* finished with\\n>" +
            "status :large_blue_circle: *DONE* on Wed, Dec 22 1999 at 09.18 AM.\\n>" + System.lineSeparator() +
            ">*<http://myhost.rs/vf/ui/pipelines/projectName/pipID|OPEN in PIPELINE DESIGNER>*";
    private static SlackNotificationService service;
    private static EnvironmentVariables environmentVariables;
    private static MockedStatic<Slack> slack;
    private static MethodsClient client;

    @BeforeAll
    static void setUp() throws Exception {
        environmentVariables = new EnvironmentVariables(
                SLACK_TOKEN.toString(), "my_token"
        );
        environmentVariables.setup();

        slack = mockStatic(Slack.class);
        Slack mockedSlack = mock(Slack.class);
        client = mock(MethodsClient.class);
        slack.when(Slack::getInstance).thenReturn(mockedSlack);
        when(mockedSlack.methods()).thenReturn(client);

        UsersListResponse foundUsers = new UsersListResponse();
        foundUsers.setMembers(List.of(
                initUser("2Uid", "user2"),
                initUser("4Uid", "user4")
        ));
        when(client.usersList(any(RequestConfigurator.class))).thenReturn(foundUsers);

        service = spy(new SlackNotificationService("template/pip_slack_template_short.template"));
    }

    @AfterAll
    static void tearDown() throws Exception {
        environmentVariables.teardown();
        slack.close();
    }

    private static User initUser(String id, String name) {
        User user = new User();
        user.setId(id);
        user.setName(name);
        return user;
    }

    @AfterEach
    public void cleanUpEach() {
        clearInvocations(service);
        clearInvocations(client);
    }

    @Test
    void testConstructNotificationBody() throws Exception {
        Optional<String> parsingResult = service.constructNotificationBody(USERNAMES_SLACK.getValue(),
                PIP_DESIGNER.getValue(), PIPELINE_NAME.getValue(), PROJECT_URL.getValue(),
                PROJECT_NAME.getValue(), STATUS.getValue(), FIXED_TIME.getValue(), DURATION.getValue());
        if (parsingResult.isPresent()) {
            assertEquals(NOTIFICATION_BODY, parsingResult.get());
        } else {
            fail();
        }
    }

    @Test
    void testSendNotificationSuccess() throws Exception {
        Notification notification = Notification.builder()
                .createdOn(Instant.now())
                .sendTo(SEND_TO_SLACK.getValue())
                .subject(SUBJECT.getValue())
                .text(NOTIFICATION_BODY)
                .build();
        ConversationsListResponse foundChannels = new ConversationsListResponse();
        foundChannels.setChannels(List.of(
                Conversation.builder().id("1id").name("develop").build(),
                Conversation.builder().id("2id").name("test").build(),
                Conversation.builder().id("3id").name("swasdii").build()
        ));
        when(client.conversationsList(any(RequestConfigurator.class))).thenReturn(foundChannels);
        ChatPostMessageResponse sendResult = new ChatPostMessageResponse();
        sendResult.setTs("example");
        when(client.chatPostMessage(any(RequestConfigurator.class))).thenReturn(sendResult);

        boolean workResult = service.sendNotification(notification);

        assertTrue(workResult, "The result should be TRUE!");
        verify(service, never()).findUsersConversations(any());

        verify(client, times(1)).conversationsList(any(RequestConfigurator.class));
        verify(client, times(3)).chatPostMessage(any(RequestConfigurator.class));
    }

    @Test
    void testSendNotificationButNotFound() throws Exception {
        Notification notification = Notification.builder()
                .createdOn(Instant.now())
                .sendTo(SEND_TO_SLACK.getValue())
                .subject(SUBJECT.getValue())
                .text(NOTIFICATION_BODY)
                .build();
        ConversationsListResponse foundChannels = new ConversationsListResponse();
        foundChannels.setChannels(Collections.emptyList());
        when(client.conversationsList(any(RequestConfigurator.class))).thenReturn(foundChannels);

        boolean workResult = service.sendNotification(notification);

        assertFalse(workResult, "The result should be FALSE!");
        verify(service, never()).findUsersConversations(any());
        verify(client, times(1)).conversationsList(any(RequestConfigurator.class));
        // There are no channels have been found.
        verify(client, never()).chatPostMessage(any(RequestConfigurator.class));
    }

    @Test
    void testSendNotificationButFailureWhenFindChannel() throws Exception {
        Notification notification = Notification.builder()
                .createdOn(Instant.now())
                .sendTo(SEND_TO_SLACK.getValue())
                .subject(SUBJECT.getValue())
                .text(NOTIFICATION_BODY)
                .build();
        when(client.conversationsList(any(RequestConfigurator.class))).thenThrow(SlackApiException.class);

        assertThrows(IllegalStateException.class, () -> service.sendNotification(notification));
        verify(service, never()).findUsersConversations(any());
        verify(client, times(1)).conversationsList(any(RequestConfigurator.class));
        // There are no channels have been found.
        verify(client, never()).chatPostMessage(any(RequestConfigurator.class));
    }

    @Test
    void testSendNotificationButFalse() throws Exception {
        Notification notification = Notification.builder()
                .createdOn(Instant.now())
                .sendTo(SEND_TO_SLACK.getValue())
                .subject(SUBJECT.getValue())
                .text(NOTIFICATION_BODY)
                .build();
        ConversationsListResponse foundChannels = new ConversationsListResponse();
        foundChannels.setChannels(List.of(
                Conversation.builder().id("1id").name("develop").build(),
                Conversation.builder().id("2id").name("test").build(),
                Conversation.builder().id("3id").name("swasdii").build()
        ));
        when(client.conversationsList(any(RequestConfigurator.class))).thenReturn(foundChannels);
        ChatPostMessageResponse sendResult = new ChatPostMessageResponse();
        sendResult.setTs("example");
        when(client.chatPostMessage(any(RequestConfigurator.class))).thenThrow(SlackApiException.class);

        boolean workResult = service.sendNotification(notification);

        assertFalse(workResult, "The result should be FALSE!");
        verify(service, never()).findUsersConversations(any());
        verify(client, times(1)).conversationsList(any(RequestConfigurator.class));
        verify(client, times(3)).chatPostMessage(any(RequestConfigurator.class));
    }
}
