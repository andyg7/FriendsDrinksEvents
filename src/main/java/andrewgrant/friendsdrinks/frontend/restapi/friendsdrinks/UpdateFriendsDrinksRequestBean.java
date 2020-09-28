package andrewgrant.friendsdrinks.frontend.restapi.friendsdrinks;

/**
 * DTO for UpdateFriendsDrinksRequest.
 */
public class UpdateFriendsDrinksRequestBean {
    private String updateType;
    private String scheduleType;
    private String cronSchedule;
    private String name;

    public UpdateFriendsDrinksRequestBean() {
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
