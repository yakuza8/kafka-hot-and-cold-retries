# Apache Kafka® - Hot and Cold Retries
A demo project for elaborating how hot and cold retries can be applied in Apache Kafka®. This repository will be the 
source and reference codebase of my blog post.

You can reach to the further details from [this link](https://medium.com/udemy-engineering/introducing-hot-and-cold-retries-on-apache-kafka-f2f23595627b)

**It includes:**
* Application configuration and configuration class to read it
* Consumer factory to create necessary listener container beans and dead letter queue forwarder
* Simple service layer which logs the upcoming messages and to create the erroneous cases
* Topic forwarding strategies and header utilization examples in those strategies
* Consumer examples for hot and cold retries

**Utilities:**
* Also, you can find a simple Kafka cluster to create your own playground before running the application 

## How to run
The repository comes with a simple Apache Kafka® cluster utilized by `docker compose`. As a preliminary condition, you 
may need to run it if there is no connectable Kafka cluster available. Otherwise, you can configure host and port
information from `application.yml`

````shell
$ cd kafka-hot-and-cold-retries
$ docker compose up # To make Kafka cluster up and running
$ gradle bootRun # To make the application up and running
````

## Flow
As depicted in the below figure, our journey starts with our producer. It publishes the message on the target topic, and
the consumer starts processing. There will not be any problem while consuming in the happy path, and the consumer will 
be able to process the message.

![Flow chart for hot and cold retry execution](diagrams/flow_of_hot_and_cold_retries.jpg?raw=true)

But, if the consumer encounters an exception, it will first check whether any hot retry is defined or not. If so, we 
will try the message consumption up until maximum hot retry attempt or it succeeds in consuming the message. If it fails
and reaches the maximum threshold, then it checks whether the cold retry configuration is defined or not. In the 
worst-case scenario where we do not have any cold retry configured, the message goes to the dead letter queue (DLQ). 
Otherwise, we will forward the message to the cold retry topic, and make it be processed with some delay. That is why 
these kinds of topics are called delayed topics.

> Note that the delayed topics do not differ from the normal topics in terms of topology but in terminology. 
You will see that they could also have a hot retry defined in them or even another cold retry topic chained next to 
another delay topic.

Now, we have forwarded the message to our cold retry topic. One of the main aims here is to chain delay topics with 
themselves or other ones without blocking the other messages in the same partition. To do that, we will implement the 
topic forwarding logic for cold retries. Hence, a failure on a cold retry topic will lead to publishing the same message
into the same topic again and skipping the current offset. Also, to have a retry threshold, we put custom headers on the
topics and check the attempt value in the next consumption. By doing so, the current offset’s message will not block the
next offset, and the current failed message will get its new offset at the end of the same partition after incrementing 
the attempt count (Yay!). Still, it is possible that we can reach the maximum attempt count limit on the cold retry. As 
a result, this will cause either publishing the message to the next cold retry topic if defined or the dead letter queue
as a sink point.

As you can see from the overall flow, the consumers will handle each forwarded message as a fresh message and apply the
hot and cold retrials on them again and again in case of failure.

