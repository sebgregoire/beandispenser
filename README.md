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


###Job failure handling

The config let's you (optionally) specify what you want to do with failed jobs. It makes the distinction between permanently failed jobs, and temporary failed jobs. Examples are, for the first, a job that needs to do something with a resource that no longer exists. The second would be for a job that uses an external API that is temporarily down. If the command that is executed can make this distinction, it can communicate this back by exiting with either an exit code of `1` for a permanent fail, or `2` for a temporary fail. `on_unknown_fail` is available for those cases where you run a script that is not yours and you don't control the exit code. In that case it would be useful to set all values to the same behaviour.

**NOTE**: if `release` is selected, jobs will be released with a backoff delay of 60 seconds to give other jobs a chance.

```
on_timeout=bury|release|delete (default:release)
on_temporary_fail=bury|release|delete (default:bury)
on_permanent_fail=bury|release|delete (default:bury)
on_unknown_fail=bury|release|delete (default:bury)
```
