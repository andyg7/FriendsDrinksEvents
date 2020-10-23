package andrewgrant.friendsdrinks.frontend.api.friendsdrinks;

/**
 * DTO for PostFriendsDrinksResponse.
 */
public class UpdateFriendsDrinksRequestBean {
    private String name;
    private String adminUserId;

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
