package andrewgrant.friendsdrinks.frontend.api.statestorebeans;

import java.util.List;

/**
 * Bean for friends drinks detail page.
 */
public class FriendsDrinksDetailPageBean {
    private String adminUserId;
    private String friendsDrinksId;
    private String name;
    List<UserStateBean> members;
    List<FriendsDrinksDetailPageMeetupBean> friendsDrinksDetailPageMeetupBeanList;

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<UserStateBean> getMembers() {
        return members;
    }

    public void setMembers(List<UserStateBean> members) {
        this.members = members;
    }

    public List<FriendsDrinksDetailPageMeetupBean> getFriendsDrinksDetailPageMeetupBeanList() {
        return friendsDrinksDetailPageMeetupBeanList;
    }

    public void setFriendsDrinksDetailPageMeetupBeanList(List<FriendsDrinksDetailPageMeetupBean> friendsDrinksDetailPageMeetupBeanList) {
        this.friendsDrinksDetailPageMeetupBeanList = friendsDrinksDetailPageMeetupBeanList;
    }
}
