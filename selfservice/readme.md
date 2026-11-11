# checks on topics before deleting

When deleting kafka topics, it is good practice to also make sure that there are no producers and consumers anymore.

Below you find an exercise, that tests whether there are still active consumer groups and whether there are events in the topic. 
This test can be build into business logic of a self service portal, hat would allow end users to administrate their own topics.



```
podman machine init --cpus 6 --memory 14336
podman machine start
```
