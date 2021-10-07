# FriendsDrinksEvents

## Technologies 
### Gradle
Used to build the application JAR.

### Kafka and Kafka Streams
Kafka Streams is used to build multiple asynchronous "micro-services". See code in https://github.com/andyg7/FriendsDrinksEvents/tree/master/src/main/java/andrewgrant/friendsdrinks - basically any class that ends in Service contains a main method that starts a Kafka Streams application. A REST API fronts interacting with the backend stream services - see https://github.com/andyg7/FriendsDrinksEvents/tree/master/src/main/java/andrewgrant/friendsdrinks/frontend. This is consumed by https://github.com/andyg7/FriendsDrinksUI.
#### Resources
- https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html
- https://docs.confluent.io/current/streams/architecture.html
### Docker
Used to build the application's Docker image.
### Kubernetes
Used for running the various containers that make up the application. There are various Kubernetes for running ZooKeeper, a schema registry, a Kafka cluster and then the actual streaming application. See https://github.com/andyg7/FriendsDrinksEvents/tree/master/kubernetes.

### Semaphore
CI/CD solution.
