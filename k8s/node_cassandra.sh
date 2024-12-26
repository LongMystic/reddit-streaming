helm install cassandra bitnami/cassandra \
    --set persistence.size=256Mi \
    --set replicaCount=1 \
    --set dbUser.user=tuanha \
    --set dbUser.password=tuanha \
    --set resources.requests.cpu=500m \
    --set resources.requests.memory=512Mi \
    --set resources.limits.cpu=1000m \
    --set resources.limits.memory=1Gi \
    --set heapSize=256m \
    --set maxHeapSize=256m \
    --set existingConfiguration=cassandra-config
