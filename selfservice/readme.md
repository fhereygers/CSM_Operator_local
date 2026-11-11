# checks on topics before deleting

When deleting kafka topics, it is good practice to also make sure that there are no producers and consumers anymore.

Below you find an exercise, that tests whether there are still active consumer groups and whether there are events in the topic. 
This test can be build into business logic of a self service portal, hat would allow end users to administrate their own topics.

Following code is executing the checks:

```
SURVEYOR_BASE="http://cloudera-surveyor-service.cld-streaming.svc.cluster.local:8080/api/v1/clusters/my-cluster"
TOPIC="telecom-cdr-avro"

echo "[*] Checking topic deletion readiness for: ${TOPIC}"

# 1. Consumer Group Check
ACTIVE_CONSUMERS=$(curl -s "${SURVEYOR_BASE}/consumer-groups?topic=${TOPIC}" | jq '[.data[]? | select(.state == "STABLE" or (.membersCount // 0) > 0)] | length')
if [ "$ACTIVE_CONSUMERS" -gt 0 ]; then
  echo "[ABORT] Topic has ${ACTIVE_CONSUMERS} ACTIVE consumer group(s) connected."
  exit 1
fi
echo "[PASS] No active consumer processes connected."

# 2. Non-Empty Check (Total Messages)
MSG_COUNT=$(curl -s "${SURVEYOR_BASE}/topics/${TOPIC}" | jq -r '.messageCount.value // 0')
echo "[*] Current message count: ${MSG_COUNT}"

if [ "$MSG_COUNT" -gt 0 ]; then
  echo "[ABORT] Topic is NOT empty (${MSG_COUNT} messages retained). Flush or purge data before deleting."
  exit 1
fi

echo "[PASS] Topic is completely empty (0 messages) and has 0 active consumers. Safe to delete."
```



```
podman machine init --cpus 6 --memory 14336
podman machine start
```


<br/>
<img src="surveyor_ui.png" width="90%">
<br/>


