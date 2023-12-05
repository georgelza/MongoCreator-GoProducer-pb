# MongoCreator - GoTransactionProducer

Hi all… as shared originally on the Discord channel, Veronica asked that I post here also…

<I do need to give this some more thought, will do a diagram of my thoughts, and then see when I I start this… just busy with some AWS training atm>.

If anyone want to potentially also be involved, welcome to ping me. happy to share the lime light, or is that blame… :wink:

G

… hi all…

no immediate schedule/plan for this… as I need to do some urgent AWS studying/work for employer…

but was thinking of doing a project and then documenting it and sharing the git repo, to the community.

Basic idea.
Golang app that generate fake sales (maybe split as a basket onto one kafka topic and then a payment onto another, implying the 2 was out of band), then sinking the topic/s into MongoDB using a sink connectors.
At this point I want to show a 2nd stream to it all, and do it via Python. was thinking…
maybe based on data sinked into the MongoDB store, do a trigger… with a do a source connector out of Mongo onto Kafka (some aggregating) and then consume that via the Python app and for simplistic just echo this to the console (implying it can be pushed somewhere further)


# Note: Not included in the repo is a file called .pwd

Example: 
export Sasl_password=Vj8MASendaIs0j4r34rsdfe4Vc8LG6cZ1XWilAJjYS05bZIk7AaGx0Y49xb 
export Sasl_username=3MZ4dfgsdfdfIUUA

This files is executed via the runs_producer.sh file, reading the values into local environment, from where they are injested by a os.Getenv call, if this code is pushed into a docker container then these values can be pushed into a secret included in the environment.