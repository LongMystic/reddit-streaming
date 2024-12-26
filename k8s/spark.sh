helm install spark bitnami/spark \
    --set master.resources.requests.cpu=500m \
    --set master.resources.requests.memory=512Mi \
    --set master.resources.limits.cpu=1000m \
    --set master.resources.limits.memory=1Gi \
    --set worker.resources.requests.cpu=500m \
    --set worker.resources.requests.memory=512Mi \
    --set worker.resources.limits.cpu=1000m \
    --set worker.resources.limits.memory=1Gi \
    --set worker.replicaCount=2
