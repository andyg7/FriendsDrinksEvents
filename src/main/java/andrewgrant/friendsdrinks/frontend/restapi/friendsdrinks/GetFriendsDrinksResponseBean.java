package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for GetFriendsDrinksResponse.
 */
public class GetFriendsDrinksResponseBean {
    private FriendsDrinksBean friendsDrinks;

    public FriendsDrinksBean getFriendsDrinks() {
        return friendsDrinks;
    }

    public void setFriendsDrinks(FriendsDrinksBean friendsDrinks) {
        this.friendsDrinks = friendsDrinks;
    }

}
