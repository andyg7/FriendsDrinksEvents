package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for GetFriendsDrinksResponse.
 */
public class GetFriendsDrinksResponseBean {
    private String name;
    private String friendsDrinksId;
    private String adminUserId;

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
