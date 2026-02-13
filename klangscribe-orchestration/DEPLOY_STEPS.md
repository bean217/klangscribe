# Steps for updating the Dagster K8s Deployment

Unfortunately the existing Dagster Helm Chart requires that updates to the code be built into a Docker image and to redeploy via Helm.

Since I do not have enough time to setup a CI/CD workflow, and also probably don't have the proper credentials to do so, I have written this guide to help you update the Dagster deployment on OpenShift!

## Obtain OpenShift Login Token `oc login`

1. Go to [CSH OKD4 Console](https://console.okd4.csh.rit.edu/)
2. In the top right, select the dropdown next to your username. Select `Copy login command`. This will give you the `oc login` command needed to connect with the CSH OpenShift cluster.
3. Run the copied command in the terminal. This will point `kubectl` commands to the CSH OpenShift cluster.

## Option 1: Manual Update (Not Recommended)

### (1) Push Code to GitHub and Rebuild/Push Dockerfile

1. Rebuild the Dockerfile:
    ```
    docker build . -t klangscribe_orchestration:<tag_id>
    ```
    - The current `<tag_id>` version should be visible on the [Klangscribe DockerHub Repo](https://hub.docker.com/r/bean217/klangscribe_orchestration)
    - Increment this value
2. Tag the build:
    ```
    docker tag klangscribe_orchestration:<tag_id> bean217/klangscribe_orchestration:<tag_id>
    ```
    - This step is redundant, but we're doing it anyways
3. Push to DockerHub:
    ```
    docker push bean217/klangscribe_orchestration:<tag_id>
    ```
    - This makes the updated docker image visible to the k8s deployment

### (2) Update the Helm Install

1. Modify the `values.yaml` tag ID 
    ```
    dagster-user-deployments.deployments[0].image.tag: <tag_id>
    ```
    - look in `~/Desktop/learn-k8s/dagster/` for the `values.yaml` file to update
    - It contains sensitive information, hence why it isn't located here.
2. Apply the deployment update to the `klangscribe` namespace:
    ```
    helm upgrade --install klangscribe dagster/dagster -f values.yaml -n klangscribe
    ```


## Option 2: Using GitHub Webhooks

This Option assumes that you used the "Import from Git" option to deploy your app in OKD.

Follow these steps to create a webhook:
1. **Developer** -> **+Add** -> **Import from Git**
2. Fill out the form to create the deployment, which creates:
    - a BuildConfig (builds the image from your repo)
    - an ImageStream (stores the built image)
    - a Deployment (runs the image)
3. Go into the created BuildConfig
4. Edit the BuildConfig to add a reference branch for the Webhook to watch:
    ```yaml
    spec:
      source:
        type: Git
          uri: 'https://github.com/<username>/<repository>.git'
          ref: main     # or another branch of your choosing     
    ```
5. Select `Copy URL with Secret` next to the webhook associated with GitHub
6. Go to your repo on GitHub -> Settings -> Webhooks -> Add webhook
    - Paste the URL you copied into `Payload URL`
    - Set `Content type` to `application/json`
    - Copy the associated secret value (from OKD) into `Secret`
    - *Temporarily* disable SSL verification

Now whenever you push to the specified branch, OKD should automatically kickoff a new build.

## Option 3: Use GitHub Actions to Build with `oc start-build` on Push

### (1) Create a service account in OpenShift for building your repo:

```bash
oc create serviceaccount <service-account-name> -n <your-namespace>
```

### (2) Allow the Service Account to Perform Actions in the Namespace:

```bash
oc policy add-role-to-user edit system:serviceaccount:<your-namespace>:<service-account-name>
```

### (3) Generate a Service Account Token and Get the OpenShift API Server URL

This will output a long JWT token:
```bash
oc create token <service-account-name> -n <your-namespace>
```

This will output the URL of the OKD cluster
```bash
oc whoami --show-server
```

### (4) Store Both the Token and Server URL in GitHub Secrets

In the Settings Menu of Your GitHub Repo:

1. Under **Security** -> **Secrets and variables** -> **Actions**
2. Add a New Repository Secret`:
    - Server URL:
        - key: `OPENSHIFT_SERVER`
        - val: `<server-url>`
    - Service Account Token:
        - key: `OPENSHIFT_TOKEN`
        - val: `<jwt-token>`

### (5) Create a GitHub Workflow in Your Repo:

In the root of your repositry:
```bash
touch .github/workflows/build.yml
```

Use the following CI-CD workflow:

```yaml
name: Trigger OpenShift Build

on:
  push:
    branches: [ "main" ]

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Install oc CLI
        uses: redhat-actions/oc-installer@v1
    
      - name: Login to OpenShift
        run: |
          oc login ${{ secrets.OPENSHIFT_SERVER }} \
            --token=${{ secrets.OPENSHIFT_TOKEN }} \
            --insecure-skip-tls-verify=true
    
      - name: Start build
        run: |
          oc start-build <build-config-name> --follow -n <your-namespace>
```

Note: you can find `<build-config-name>` under the `metadata` section of the BuildConfig yaml file in OKD.