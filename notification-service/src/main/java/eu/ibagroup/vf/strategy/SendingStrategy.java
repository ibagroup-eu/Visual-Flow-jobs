package eu.ibagroup.vf.strategy;

import eu.ibagroup.vf.model.Notification;

import java.util.Optional;

/**
 * Interface, contains common logic for all sending strategies.
 */
public interface SendingStrategy {

    /**
     * Method for getting template file for notification body and parsing it.
     * @param params values, used while parsing a template file.
     * @return completed body for notification.
     */
    Optional<String> constructNotificationBody(String ... params);

    /**
     * Common method for sending a notification for all sending strategies.
     * @param notification is a notification object.
     * @return true, if the notification was successfully sent, otherwise - false.
     */
    boolean sendNotification(Notification notification);
}
