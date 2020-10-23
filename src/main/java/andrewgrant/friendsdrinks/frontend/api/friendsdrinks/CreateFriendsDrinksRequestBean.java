package andrewgrant.friendsdrinks.frontend.api.friendsdrinks;

/**
 * DTO for CreateFriendsDrinks request.
 */
public class CreateFriendsDrinksRequestBean {
    private String name;
    private String adminUserId;

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }


    public CreateFriendsDrinksRequestBean() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
