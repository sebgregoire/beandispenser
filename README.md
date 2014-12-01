Bean Dispenser
==============

###NOTE: still in development. Don't use in production.

A configurable python process to run unix processes for received jobs from beanstalkd queues. The motivation behind this project is that I found myself needing a centralized place to manage my beanstalkd workers. Having tens of little workers, all doing their own "while true" loops became unmanagable very quickly. Also, if you're using PHP for your workers, you may not want to implement the long running process that watches the tubes in PHP ([here](http://software-gunslinger.tumblr.com/post/47131406821/php-is-meant-to-die)'s a fun read on why).

###Configuration:

example:

```yaml
connection:
        host: localhost
        port: 11300

error_codes:
        database_error: 1
        api_error: 2
        user_doesnt_exit: 3

tubes:
      - name: foo
        command: /path/to/command
        workers: 5
        error_handling:
                database_error: release
                api_error: release
                user_doesnt_exit: delete
```