package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for CreateFriendsDrinks response.
 */
public class CreateFriendsDrinksResponseBean {
    private String result;
    private FriendsDrinksIdBean friendsDrinksId;

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public FriendsDrinksIdBean getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(FriendsDrinksIdBean friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }
}
