package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for GetFriendsDrinksResponse.
 */
public class GetFriendsDrinksResponseBean {
    private String name;
    private FriendsDrinksIdBean friendsDrinksId;

    public FriendsDrinksIdBean getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(FriendsDrinksIdBean friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
