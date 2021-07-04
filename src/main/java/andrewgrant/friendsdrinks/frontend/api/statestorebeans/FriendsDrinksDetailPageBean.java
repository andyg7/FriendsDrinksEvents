package andrewgrant.friendsdrinks.frontend.api.statestorebeans;

import java.util.List;

/**
 * Bean for friends drinks detail page.
 */
public class FriendsDrinksDetailPageBean {
    private String adminUserId;
    private String friendsDrinksId;
    private String name;
    List<UserStateBean> memberList;
    List<FriendsDrinksDetailPageMeetupBean> friendsDrinksDetailPageMeetupList;

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

    public List<UserStateBean> getMemberList() {
        return memberList;
    }

    public void setMemberList(List<UserStateBean> memberList) {
        this.memberList = memberList;
    }

    public List<FriendsDrinksDetailPageMeetupBean> getFriendsDrinksDetailPageMeetupList() {
        return friendsDrinksDetailPageMeetupList;
    }

    public void setFriendsDrinksDetailPageMeetupList(List<FriendsDrinksDetailPageMeetupBean> friendsDrinksDetailPageMeetupList) {
        this.friendsDrinksDetailPageMeetupList = friendsDrinksDetailPageMeetupList;
    }
}
