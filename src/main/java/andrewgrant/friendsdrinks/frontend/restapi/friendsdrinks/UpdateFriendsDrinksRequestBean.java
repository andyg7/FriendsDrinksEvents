package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

import java.util.List;

/**
 * DTO for UpdateFriendsDrinksRequest.
 */
public class UpdateFriendsDrinksRequestBean {
    private String updateType;
    private String friendsDrinksId;
    private String adminUserId;
    private List<String> userIds;
    private String scheduleType;
    private String cronSchedule;
    private String name;

    public UpdateFriendsDrinksRequestBean() {
    }

    public String getFriendsDrinksId() {
        return friendsDrinksId;
    }

    public void setFriendsDrinksId(String friendsDrinksId) {
        this.friendsDrinksId = friendsDrinksId;
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

}
