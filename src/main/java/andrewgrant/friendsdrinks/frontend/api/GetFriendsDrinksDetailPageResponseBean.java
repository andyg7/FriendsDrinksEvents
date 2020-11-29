package andrewgrant.friendsdrinks.frontend.api;

import java.util.List;

import andrewgrant.friendsdrinks.frontend.api.user.UserBean;

/**
 * DTO for GetFriendsDrinksDetailPageResponse.
 */
public class GetFriendsDrinksDetailPageResponseBean {
    private String adminUserId;
    private String friendsDrinksId;
    private String name;
    private List<UserBean> members;

    public List<UserBean> getMembers() {
        return members;
    }

    public void setMembers(List<UserBean> members) {
        this.members = members;
    }

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

}
