# Some Kafka Connect Notes

Simple, all our sink and eventually source jobs will be defined here.

Note our MongoDB Atlast credentials are pulled in via a .pwdmongoatlas and .pwdmongolocal file executed by the cremongosinks.sh. This is to "exclude" the creds from the git sink. The creds are injected into the sinks using a simple environment variable.


