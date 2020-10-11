package andrewgrant.friendsdrinks.frontend.restapi.api.user;

import java.util.List;

/**
 * DTO for GetUsersResponse.
 */
public class GetUsersResponseBean {
    private List<UserBean> users;

    public List<UserBean> getUsers() {
        return users;
    }

    public void setUsers(List<UserBean> users) {
        this.users = users;
    }


}
