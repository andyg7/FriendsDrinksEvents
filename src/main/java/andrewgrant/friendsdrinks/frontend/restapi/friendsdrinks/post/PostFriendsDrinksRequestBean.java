package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks.post;

/**
 * DTO for UpdateFriendsDrinksRequest.
 */
public class PostFriendsDrinksRequestBean {
    // Defaults to partial update of FriendsDrinks.
    // Other options: ADD_FRIEND
    private String updateType;
    private String scheduleType;
    private String cronSchedule;
    private String name;
    private String type;
    // Only relevant for ADD_FRIEND
    private String userId;

    public PostFriendsDrinksRequestBean() {
    }

    public String getScheduleType() {
        return scheduleType;
    }

    public void setScheduleType(String scheduleType) {
        this.scheduleType = scheduleType;
    }

    public String getCronSchedule() {
        return cronSchedule;
    }

    public void setCronSchedule(String cronSchedule) {
        this.cronSchedule = cronSchedule;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUpdateType() {
        return updateType;
    }

    public void setUpdateType(String updateType) {
        this.updateType = updateType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
