package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for CreateFriendsDrinks request.
 */
public class CreateFriendsDrinksRequestBean {
    private String friendsDrinkdsId;
    private String adminUserId;
    private List<String> userIds;
    private String scheduleType;
    private String cronSchedule;

    public CreateFriendsDrinksRequestBean() {

    }

    public String getFriendsDrinkdsId() {
        return friendsDrinkdsId;
    }

    public void setFriendsDrinkdsId(String friendsDrinkdsId) {
        this.friendsDrinkdsId = friendsDrinkdsId;
    }

    public String getAdminUserId() {
        return adminUserId;
    }

    public void setAdminUserId(String adminUserId) {
        this.adminUserId = adminUserId;
    }

    public List<String> getUserIds() {
        return userIds;
    }

    public void setUserIds(List<String> userIds) {
        this.userIds = userIds;
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
}
