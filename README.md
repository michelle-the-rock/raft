Implementation of the Raft consensus algorithm for CS3700 Networks and Distributed Systems at Northeastern University, Spring 2019

https://raft.github.io/raft.pdf

Runs on a simulator built by Professor Christo Wilson on the Northeastern's Linux machines.
Several copies of this code are run (replicas) and they work together to elect a leader
and maintain a consistent database of key-value pairs. 
The simulator can be calibrated to send set proportions of Get versus Put messages, 
and can be calibrated to kill either leader or follower replicas, 
or erect a network partition that prevents some replicas from speaking to others.
The algorithm is designed to be robust against all of these events, 
prioritizing consistency and partition tolerance over availability (according to the CAP theorem).
