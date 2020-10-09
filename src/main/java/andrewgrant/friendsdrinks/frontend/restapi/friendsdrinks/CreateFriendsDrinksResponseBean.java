package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for CreateFriendsDrinks response.
 */
public class CreateFriendsDrinksResponseBean {
    private String result;
    private String friendsDrinksId;

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
    }


    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }


    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

}
