package andrewgrant.friendsdrinks.frontend.restapi.emails;

import java.util.List;

/**
 * DTO for GetUsersResponseBean.
 */
public class GetEmailsResponseBean {
    private List<String> emails;

    public List<String> getEmails() {
        return emails;
    }

    public void setEmails(List<String> emails) {
        this.emails = emails;
    }

}
