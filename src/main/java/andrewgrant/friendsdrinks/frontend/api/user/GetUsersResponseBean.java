package andrewgrant.friendsdrinks.frontend.api.user;

import java.util.List;

import andrewgrant.friendsdrinks.frontend.api.statestorebeans.UserStateBean;

/**
 * DTO for GetUsersResponse.
 */
public class GetUsersResponseBean {
    private List<UserStateBean> users;

    public List<UserStateBean> getUsers() {
        return users;
    }

    public void setUsers(List<UserStateBean> users) {
        this.users = users;
    }
}
