package eu.ibagroup.vf.util;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public enum TestParams {
    SEND_TO_EMAIL("recipient@mail.rs"),
    SEND_TO_SLACK("#develop,#test,#swasdii"),
    SUBJECT("subject"),
    USERNAMES_SLACK("user1,user2,user3,user4"),
    HOST("http://myhost.rs"),
    PIPELINE_ID("pipID"),
    PIPELINE_NAME("pipName"),
    PROJECT_NAME("projectName"),
    STATUS("DONE"),
    PIP_DESIGNER("http://myhost.rs/vf/ui/pipelines/projectName/pipID"),
    PROJECT_URL("http://myhost.rs/vf/ui/projectName/overview"),
    FIXED_TIME("1999-12-22T09:14:44Z"),
    DURATION("234.142");

    private final String value;
}
