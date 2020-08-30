package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for GetFriendsDrinksResponseBean.
 */
public class GetFriendsDrinksResponseBean {
    private List<FriendsDrinksBean> adminFriendsDrinks;
    private List<FriendsDrinksBean> memberFriendsDrinks;

    public List<FriendsDrinksBean> getAdminFriendsDrinks() {
        return adminFriendsDrinks;
    }

    public void setAdminFriendsDrinks(List<FriendsDrinksBean> adminFriendsDrinks) {
        this.adminFriendsDrinks = adminFriendsDrinks;
    }

    public List<FriendsDrinksBean> getMemberFriendsDrinks() {
        return memberFriendsDrinks;
    }

    public void setMemberFriendsDrinks(List<FriendsDrinksBean> memberFriendsDrinks) {
        this.memberFriendsDrinks = memberFriendsDrinks;
    }

}
