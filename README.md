Bean Dispenser
==============

Bean Dispenser is a configurable python process to run UNIX executables for received jobs from beanstalkd tubes, passing the job body to the STDIN of the executable. In the configuration file you specify which tubes you want to listen to, how many workers you want to put on it, and what you want to do in case a jobs fails (exits with code > 0, or doesn't finish within TTR). The advantage of this is that you have all your worker configurations in one place and all your application logic is executed in short running processes.

Also, in case you're using PHP for your workers, you may not want to implement the long running process that watches the tubes in PHP ([here](http://software-gunslinger.tumblr.com/post/47131406821/php-is-meant-to-die)'s a fun read on why).


### Requirements

Before running Beandispenser, a couple of preparations need to be made. First, two python libraries need to be installed (you can use pip to install these): **pyYAML** (for beanstalkc and reading the configuration file) and **beandstalkc** (for all interactions with beandstalk).

Second, an environment variable `BEANDISPENDER_CONFIG_FILE` needs to be set, specifying the path to the configuration file (see below for an example).

### Usage

Beandispenser does not manage daemonizing. It's up to the user to choose their favorite solution (supervisor, upstart, systemd...). Start Beandispenser with `python /path/to/beandispenser.py`. This will read your config file, throw any errors if an error is found in your config file, and when all is in order, it will start forking the number of workers you specified for each tube in your configuration.


###Configuration:

The configuration file uses the YAML format. Two sections are required: `connection` and `tubes`.

####Defining tubes

The `tubes` section defines which tubes you want to listen on, and what should be done with jobs that become available. The format is as follows:

```yaml
tubes:
      - name: tube_name (required)
        command: /path/to/command (required)
        workers: number_of_workers (optional. default: 1)
        error_handling: hash_of_error_names_and_job_actions (optional. default: {})
```

Example:

```yaml
connection:
        host: localhost
        port: 11300

error_codes:
        database_error: 1
        api_error: 2
        user_doesnt_exist: 3

tubes:
      - name: foo
        command: /path/to/command
        workers: 5
        error_handling:
                database_error: release
                api_error: release
                user_doesnt_exit: delete
```
