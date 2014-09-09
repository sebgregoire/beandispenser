Bean Dispenser
==============


A configurable python process to run unix processes for received jobs from beanstalkd queues.

###Configuration:

The configuaration file has one `[connection]` section and multiple `[pool:poolname]` sections. The connection section specifies the connection to your beanstalkd server. Each pool section specifies which tube to watch, how many worker processes to allow at the same time and what to do with the received jobs.

An example:

```conf
[connection]
host=localhost
port=11300

[pool:foo]
workers=3
command=/path/to/command

[pool:bar]
workers=5
command=/path/to/command
```

Two optional pool configuration options are available to specifiy what to do with failed jobs and jobs that take longer than the ttr of the job. There options are:

```
on_timeout=bury|release (default:release)
on_fail=bury|release (default:bury)
```
