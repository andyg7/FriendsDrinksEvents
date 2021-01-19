package andrewgrant.friendsdrinks.frontend.api.meetup;

import java.util.List;

import andrewgrant.friendsdrinks.frontend.api.user.UserBean;

/**
 * Bean for Meetup.
 */
public class MeetupBean {
    private List<UserBean> users;
    private String date;

    public void setDate(String date) {
        this.date = date;
    }

    public String getDate() {
        return date;
    }

    public List<UserBean> getUsers() {
        return users;
    }

    public void setUsers(List<UserBean> users) {
        this.users = users;
    }

}
