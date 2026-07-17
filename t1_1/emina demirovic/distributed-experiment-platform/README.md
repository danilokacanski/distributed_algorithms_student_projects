# Distributed Experiment Platform

This repository contains the working implementation of a distributed platform for executing and monitoring machine learning experiments.

The project focuses on the system-level organization of experiment execution rather than on the development of a new machine learning algorithm. The main idea is to provide a prototype architecture in which experiments can be submitted, assigned to available worker nodes, executed, monitored and recorded in a structured way.

The platform is based on a coordinator-worker model. The coordinator is responsible for managing experiments, tracking worker availability, assigning tasks and recording execution events. Worker nodes are responsible for executing assigned experiments and reporting their status, metrics and results.

The prototype is intended to demonstrate key mechanisms relevant to distributed experiment execution, including worker registration, heartbeat-based availability tracking, experiment status management, result logging and basic fault handling. A consensus-based coordination layer may also be considered for maintaining a consistent ordering of important command events in the system.

This repository is part of a master’s thesis project and is currently under active development.
