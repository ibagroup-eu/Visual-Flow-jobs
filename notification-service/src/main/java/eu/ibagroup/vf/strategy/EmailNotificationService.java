package eu.ibagroup.vf.strategy;

import eu.ibagroup.vf.model.Notification;
import eu.ibagroup.vf.util.CommonUtils;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

import static eu.ibagroup.vf.model.EnvParam.*;
import static eu.ibagroup.vf.util.CommonUtils.INPUT_DATETIME_FORMAT;
import static eu.ibagroup.vf.util.CommonUtils.OUTPUT_DATETIME_FORMAT;

/**
 * Class, contains methods for sending notifications to emails.
 */
@Slf4j
@RequiredArgsConstructor
public class EmailNotificationService implements SendingStrategy {

    private final String template;
    private static final String HTML_PIPELINE = "pipelineName";
    private static final String HTML_PROJECT_NAME = "projectName";
    private static final String HTML_PROJECT_URL = "projectURL";
    private static final String HTML_STATUS = "status";
    private static final String HTML_PIP_DESIGNER = "pipelineDesignerURL";
    private static final String HTML_YEAR = "currentYear";
    private static final String HTML_FINISHED_AT = "finishedAt";

    @Override
    public Optional<String> constructNotificationBody(String ... params) {
        TemplateEngine templateEngine = new TemplateEngine();
        ClassLoaderTemplateResolver resolver = new ClassLoaderTemplateResolver();
        resolver.setCharacterEncoding("UTF-8");
        resolver.setTemplateMode(TemplateMode.HTML);
        templateEngine.setTemplateResolver(resolver);
        Context ct = new Context();
        ct.setVariable(HTML_PIPELINE, params[2]);
        ct.setVariable(HTML_PIP_DESIGNER, params[1]);
        ct.setVariable(HTML_PROJECT_NAME, params[4]);
        ct.setVariable(HTML_PROJECT_URL, params[3]);
        ct.setVariable(HTML_STATUS, params[5]);
        LocalDateTime startedAt = LocalDateTime.parse(params[6], DateTimeFormatter.ofPattern(INPUT_DATETIME_FORMAT));
        LocalDateTime finishedAt = startedAt.plusSeconds(Double.valueOf(params[7]).longValue());
        ct.setVariable(HTML_FINISHED_AT, finishedAt.format(DateTimeFormatter.ofPattern(
                OUTPUT_DATETIME_FORMAT, Locale.ENGLISH)));
        ct.setVariable(HTML_YEAR, String.valueOf(finishedAt.getYear()));
        return Optional.of(templateEngine.process(template, ct));
    }

    @Override
    public boolean sendNotification(Notification notification) {
        final String senderEmail = CommonUtils.getEnvProperty(EMAIL_ADDRESS);
        final String senderPassword = CommonUtils.getEnvProperty(EMAIL_PASS);
        final Properties properties = new Properties();
        properties.put("mail.smtp.host", CommonUtils.getEnvProperty(SMTP_HOST));
        properties.put("mail.smtp.port", CommonUtils.getEnvProperty(SMTP_PORT));
        properties.put("mail.smtp.starttls.enable", CommonUtils.getEnvProperty(SMTP_STARTTLS));
        properties.put("mail.smtp.auth", CommonUtils.getEnvProperty(SMTP_AUTH));

        Session session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(senderEmail, senderPassword);
            }
        });
        session.setDebug(true);

        try {
            MimeMessage emailMessage = new MimeMessage(session);
            emailMessage.setFrom(new InternetAddress(senderEmail));
            emailMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(notification.getSendTo()));
            emailMessage.setSubject(notification.getSubject());
            emailMessage.setContent(notification.getText(), "text/html");
            LOGGER.info("Sending notification from {} to {}", senderEmail, notification.getSendTo());
            Transport.send(emailMessage);
            LOGGER.info("The notification was sent successfully!");
        } catch (Exception e) {
            LOGGER.error("An error has occurred during sending a letter: ", e);
            throw new IllegalStateException("Unable to connect to Email Account", e);
        }
        return true;
    }
}