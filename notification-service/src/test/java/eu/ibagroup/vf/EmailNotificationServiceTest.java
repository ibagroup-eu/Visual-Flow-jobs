package eu.ibagroup.vf;

import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetupTest;
import eu.ibagroup.vf.model.Notification;
import eu.ibagroup.vf.strategy.EmailNotificationService;
import jakarta.mail.internet.MimeMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import java.time.Instant;
import java.util.Optional;
import static eu.ibagroup.vf.model.EnvParam.*;
import static eu.ibagroup.vf.util.TestParams.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class EmailNotificationServiceTest {
    private static final String NOTIFICATION_BODY = "<h4>Hello, <span id=\"user\">Dear User</span>!</h4>" + System.lineSeparator() +
            "<p>" + System.lineSeparator() +
            "  We would like to say that pipeline <a id=\"pipeline\" href=\"#\">PIPELINE_NAME</a> of project" + System.lineSeparator() +
            "  <a id=\"project\" href=\"#\">PROJECT_NAME</a> finished with status <strong><span id=\"status\">STATUS</span></strong> on <span id=\"datetime\">FINISHED_AT</span>." + System.lineSeparator() +
            "</p>" + System.lineSeparator() +
            "<div class=\"btn-container\">" + System.lineSeparator() +
            "  <a class=\"btn\" id=\"pipeline_designer\" href=\"#\">OPEN in PIPELINE DESIGNER</a>" + System.lineSeparator() +
            "</div>" + System.lineSeparator() +
            "<p>" + System.lineSeparator() +
            "  This email was auto generated and sent by Visual Flow. <br />" + System.lineSeparator() +
            "  So if you want to contact us do not reply this email." + System.lineSeparator() +
            "</p>" + System.lineSeparator() +
            "<p>" + System.lineSeparator() +
            "  <span id=\"current_year\">CURRENT_YEAR</span>. Visual Flow" + System.lineSeparator() +
            "</p>";
    private static EmailNotificationService service;
    private static EnvironmentVariables environmentVariables;
    private static GreenMail greenMail;

    @BeforeAll
    static void setUp() {
        service = new EmailNotificationService("template/pip_email_template_short.html");
    }

    @AfterAll
    static void tearDown() throws Exception {
        environmentVariables.teardown();
        greenMail.stop();
    }

    static void setUpServer() throws Exception {
        greenMail = new GreenMail(ServerSetupTest.SMTP);
        greenMail.start();
        environmentVariables = new EnvironmentVariables(
                EMAIL_ADDRESS.toString(), "sender@mail.com",
                EMAIL_PASS.toString(), "123456",
                SMTP_HOST.toString(), "localhost",
                SMTP_PORT.toString(), "3025",
                "SMTP_STARTTLS", "false",
                SMTP_AUTH.toString(), "false");
        environmentVariables.setup();
    }


    @Test
    void testConstructNotificationBody() {
        Optional<String> parsingResult = service.constructNotificationBody(USERNAMES_SLACK.getValue(),
                PIP_DESIGNER.getValue(), PIPELINE_NAME.getValue(), PROJECT_URL.getValue(),
                PROJECT_NAME.getValue(), STATUS.getValue(), FIXED_TIME.getValue(), DURATION.getValue());
        if (parsingResult.isPresent()) {
            assertEquals(NOTIFICATION_BODY.replaceAll("\r\n", ""),
                    parsingResult.get().replaceAll("\r\n", ""));
        } else {
            fail();
        }
    }

    @Test
    void testSendMail() throws Exception {
        setUpServer();
        Notification notification = Notification.builder()
                .createdOn(Instant.now())
                .sendTo(SEND_TO_EMAIL.getValue())
                .subject(SUBJECT.getValue())
                .text(NOTIFICATION_BODY)
                .build();
        boolean sendResult = service.sendNotification(notification);
        assertTrue(greenMail.waitForIncomingEmail(5000, 1));
        assertTrue(sendResult, "Sending a message should be successful!");
        MimeMessage[] messages = greenMail.getReceivedMessages();
        assertEquals(1, messages.length);
        assertEquals(SUBJECT.getValue(), messages[0].getSubject());
        String body = GreenMailUtil.getBody(messages[0]).replaceAll("=\r?\n", "");
        assertEquals(NOTIFICATION_BODY, body);
    }
}
