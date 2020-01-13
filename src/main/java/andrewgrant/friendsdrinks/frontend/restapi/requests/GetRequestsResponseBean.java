package andrewgrant.friendsdrinks.frontend.restapi.requests;

import java.util.List;

/**
 * DTO for GetUsersResponseBean.
 */
public class GetRequestsResponseBean {
    List<String> requests;

    public GetRequestsResponseBean() {
    }

    public List<String> getRequests() {
        return requests;
    }

    public void setRequests(List<String> requests) {
        this.requests = requests;
    }
}
