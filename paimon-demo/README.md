A Flink application project using Java and Gradle.

To package your job for submission to Flink, use: 'gradle shadowJar'. Afterwards, you'll find the
jar to use in the 'build/libs' folder.

To run and test your application with an embedded instance of Flink use: 'gradle run'




# Setup

## Deploy the Flink operator

```bash
kubectl config use-context docker-desktop

kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml

helm repo remove flink-operator-repo # if needed
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator --namespace ng-flink --create-namespace --set watchNamespaces={ng-flink}

kubectl create -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-1.8/examples/basic.yaml -n ng-flink

kubectl get pods -n ng-flink
kubectl logs -f deploy/basic-example -n ng-flink

kubectl port-forward svc/basic-example-rest 8081 -n ng-flink

kubectl delete flinkdeployment/basic-example -n ng-flink
```

