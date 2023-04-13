package eu.ibagroup.vf.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * Common class for all notifications sending types.
 * Use this and only this.
 */
@Data
@Builder
public class Notification {
    private String sendTo;
    private String subject;
    private String text;
    private Instant createdOn;
}
