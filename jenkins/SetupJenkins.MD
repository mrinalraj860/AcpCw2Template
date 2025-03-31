# Setup Jenkins and a pipeline

## Guidelines: 
https://www.docker.com/blog/docker-and-jenkins-build-robust-ci-cd-pipelines/


## Install & Build
`https://www.jenkins.io/doc/book/installing/docker/`

Create a bridge network
`docker network create jenkins`


In order to execute Docker commands inside Jenkins nodes, download and run the docker:dind Docker image using the following docker run command

`docker run \
  --name jenkins-docker \
  --rm \
  --detach \
  --privileged \
  --network jenkins \
  --network-alias docker \
  --env DOCKER_TLS_CERTDIR=/certs \
  --volume jenkins-docker-certs:/certs/client \
  --volume jenkins-data:/var/jenkins_home \
  --publish 2376:2376 \
  docker:dind \
  --storage-driver overlay2`

Customize the official Jenkins Docker image, by executing the following:

`docker build -t acp-jenkins:lts .`

Run the image

`docker run \
  --name jenkins-blueocean \
  --restart=on-failure \
  --detach \
  --network jenkins \
  --env DOCKER_HOST=tcp://docker:2376 \
  --env DOCKER_CERT_PATH=/certs/client \
  --env DOCKER_TLS_VERIFY=1 \
  --publish 8100:8080 \
  --publish 50000:50000 \
  --volume jenkins-data:/var/jenkins_home \
  --volume jenkins-docker-certs:/certs/client:ro \
  acp-jenkins:lts`


This will create the Jenkins structure, etc. 


**Now the plugins can be loaded:**

- Docker Pipeline Plugin
- Docker
- CloudBees Docker Build and Publish

**Next steps**
- Now set the maven tool
- Specify docker hub connectivity

Then the pipeline can be created and run:  [jenkins-pipeline](jenkins-pipeline)

Polling for the SCM can be defined using: H/2 * * * *
Even easier would be an automated build every x minutes


