Bean Dispenser
==============

###NOTE: still in development. Don't use in production.

A configurable python process to run unix processes for received jobs from beanstalkd queues. The motivation behind this project is that I found myself needing a centralized place to manage my beanstalkd workers. Having tens of little workers, all doing their own "while true" loops became unmanagable very quickly. Also, if you're using PHP for your workers, you may not want to implement the long running process that watches the tubes in PHP ([here](http://software-gunslinger.tumblr.com/post/47131406821/php-is-meant-to-die)'s a fun read on why).


###Installation
Bean Dispenser can be installed using pip. To install, clone the repo, cd into it and run `pip install .`. This will install all dependencies, create an upstart script, and place a config file at `/etc/default/beandispenser`. Bean Dispenser will start at boot using the default upstart script, but if you don't want this behaviour, you can modify the upstart script at `/etc/init/beandispenser.conf`.


###Configuration:

The configuaration file has one `[connection]` section and multiple `[pool:poolname]` sections. The connection section specifies the connection to your beanstalkd server. Each pool section specifies which tube to watch, how many worker processes to allow at the same time and what to do with the received jobs.

An example:

```
[connection]
host=localhost
port=11300

[tube:foo]
workers=3
command=/path/to/command

[tube:bar]
workers=5
command=/path/to/command
```

Two optional pool configuration options are available to specifiy what to do with failed jobs and jobs that take longer than the ttr of the job. There options are:

```
on_timeout=bury|release (default:release)
on_fail=bury|release (default:bury)
```
