package andrewgrant.friendsdrinks.frontend.api.state;

import java.util.List;

/**
 * Bean for FriendsDrinksDetailPageMeetup.
 */
public class FriendsDrinksDetailPageMeetupBean {
    private String date;
    List<UserStateBean> userStateBeanList;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public List<UserStateBean> getUserStateBeanList() {
        return userStateBeanList;
    }

    public void setUserStateBeanList(List<UserStateBean> userStateBeanList) {
        this.userStateBeanList = userStateBeanList;
    }
}
