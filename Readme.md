# Problem Statement:

![img.png](img.png)

# Solution Proposed

![solution.png](..%2F..%2F..%2F..%2FDownloads%2Fsolution.png)

## FrontEnd:

#### Responsible for
- consuming messages from Kafka
- orders all message by time
- micro-batch it by Category (RGB)
- forward it to backend for set preparation
- receives RetirementRequest from current serving SetCollector
- Firstly the SetCollector is moved to old collectors (data still available in memory)
- Creates new SetCollector, now it is responsible for forming sets
- RGB manager prunes aged actor based on configuration. ex: keep 10 aged actor. if 11th added to agedList. remove 1st.
- so it is based on user to decide how many data to keep.
- across actor query coordination. Ex: given start, end time could be spanned across multiple actors. it takes care of 
- coordinating and producing the results. 

## Backend 
#### - one active backend at a time

- receives batch of sorted events by manager
- fills red 
- if any out of order events (G, B -> before R or B before G), buffers it and forms sets later the required event arrived.
- fills green and blue based on condition
- for given time range - does binary search and returns the result.
- if max no of sets served, requests for retirement

## Lets get the hands dirty:

To run the unit testing

```sh
sbt test
```
To run the app

```sh
docker compose up -d
```

To run the app in k8s

```sh
docker compose up -d
```

## Future Work

#### - Clustered approach to scale out the backend. so we can accommodate more in-memory data across multiple machines. 