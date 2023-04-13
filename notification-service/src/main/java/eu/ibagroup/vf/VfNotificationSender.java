package eu.ibagroup.vf;

import eu.ibagroup.vf.model.Notification;
import eu.ibagroup.vf.model.SendingType;
import eu.ibagroup.vf.strategy.EmailNotificationService;
import eu.ibagroup.vf.strategy.SendingStrategy;
import eu.ibagroup.vf.strategy.SlackNotificationService;
import eu.ibagroup.vf.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import java.time.Instant;
import java.util.Optional;
import static eu.ibagroup.vf.model.EnvParam.VF_HOST;

/**
 * Main class of the notification job.
 */
@Slf4j
public class VfNotificationSender {
    /**
     * Secondary method, determines sending method.
     * @param type is sending type, based on which it is needed to determine sending strategy.
     * @return determined strategy.
     */
    private static SendingStrategy initSendingType(SendingType type) {
        switch (type) {
            case EMAIL:
                return new EmailNotificationService("template/pip_email_template.html");
            case SLACK:
                return new SlackNotificationService("template/pip_slack_template_short.template");
            default:
                throw new IllegalArgumentException("There is no services implemented this sending type!");
        }
    }

    /**
     * Job entrypoint.
     * @param args is command line arguments.
     */
    public static void main(String[] args) {
        SendingType sendStrategy = SendingType.valueOf(args[0]);
        String sendTo = args[1];
        String subject = args[2];
        String mentionedUsers = args[3];
        String host = CommonUtils.getEnvProperty(VF_HOST);
        String pipelineID = args[4];
        String pipelineName = args[5];
        String projectName = args[6];
        String status = args[7];
        String startedAt = args[8];
        String pipelineDuration = args[9];
        String pipelineDesignerURL = String.format("%s/vf/ui/pipelines/%s/%s", host, projectName, pipelineID);
        String projectURL = String.format("%s/vf/ui/%s/overview", host, projectName);

        if (sendTo != null && !sendTo.isEmpty()) {
            SendingStrategy service = initSendingType(sendStrategy);
            Optional<String> notificationBody = service.constructNotificationBody(mentionedUsers,
                    pipelineDesignerURL, pipelineName, projectURL, projectName, status, startedAt, pipelineDuration);
            if (notificationBody.isPresent()) {
                Notification notification = Notification.builder()
                        .createdOn(Instant.now())
                        .sendTo(sendTo)
                        .subject(subject)
                        .text(notificationBody.get())
                        .build();
                service.sendNotification(notification);
            }
        }
    }
}
