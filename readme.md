# Iterum Manager

This repository contains the code with regards to the cluster manager of Iterum. This software artifact contains the source code with regards to the pipeline management, and the provenance tracker of the Iterum architecture.  

# Setting up
 
A prebuild docker image for the manager is present on [DockerHub](https://hub.docker.com/u/iterum). The manager is one of the software artifacts of the Iterum framework, and requires the other software artifacts to be present in the cluster to function properly. A general overview of the Iterum framework can be found [here](https://github.com/iterum-provenance/iterum). 

The daemon can be deployed on a Kubernetes cluster using the instructions stated in the [cluster repository](https://github.com/iterum-provenance/cluster). 


# Code organization

The source code of the manager is split into a couple of different modules. First there are API modules, used to communicate with different services present in the cluster:
* *daemon*, to communicate with the [daemon](https://github.com/iterum-provenance/daemon)
* *kube*, to communicate with the [Kubernetes API](https://kubernetes.io/docs/concepts/overview/kubernetes-api/)
* *mq*, to communicate with the message queing system (RabbitMQ)

Additionally there are the following modules:
* *pipeline*, consisting of the source code for an actor which manages a running pipeline
* *provenance_tracker*, which consists of the source code for an actor which manages the provenance information during a running pipeline.


These top-level modules are further split into submodules. This is further explained in the lower-level code documentation. The command `cargo doc --no-deps --open` can be run to compile the documentation for this project. The command opens up a browsable site where the code documentation for each module can be read.