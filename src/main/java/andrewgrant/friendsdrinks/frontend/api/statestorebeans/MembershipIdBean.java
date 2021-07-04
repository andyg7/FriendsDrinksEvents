package andrewgrant.friendsdrinks.frontend.api.statestorebeans;

/**
 * Bean for membership ID.
 */
public class MembershipIdBean {
   private String friendsDrinksId;
   private String userId;

   public String getFriendsDrinksId() {
      return friendsDrinksId;
   }

   public void setFriendsDrinksId(String friendsDrinksId) {
      this.friendsDrinksId = friendsDrinksId;
   }

   public String getUserId() {
      return userId;
   }

   public void setUserId(String userId) {
      this.userId = userId;
   }
}
