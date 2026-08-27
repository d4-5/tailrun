# tailrund

Logs health of every worker every 5 (configurable) seconds
Sends jobs from queue to available worker, receives job logs from worker after it's finished
Joins a Tailscale tailnet with `--auth-key` and prints the controller URL on startup

## Endpoints

- POST /tasks
- GET /tasks
- GET /tasks/{id}
- POST /tasks/{id}/logs
- GET /tasks/{id}/logs
- POST /workers
- GET /workers
- GET /workers/{id}

# tailrun-worker

Configured with controller url and auth key
Registers itself with controller by using POST /worker endpoint where it send it's configuration
Accepts the tasks (command), runs it and sends log back to the controller
Enforces that one machine can run only one worker

## Endpoints

- POST /task - controller sends job
- GET /health - returns cpu load, ram usage, storage usage

# Other

Web UI
CLI (tailrun)
Web UI if provided tailscale api key can generate command for starting worker node with automatically generated auth key

# TODO
figure out buffered channel size
python's multiprocessing replacement
there is tailscale wasm client
implement sysinfo.go
http server and client config values
add check if there is already tsnet running, and if so user should specify different tsnet-dir
check frotend
filter for http handlers
Persist state across restarts
unit tests, integration tests, e2e tests
add stuff to README.md

figure out ticker time in event borker
How IdleTimeout works?
Should we actually ingore errors in defer for example or log them not just in debug mode????????
JSON decoding does not reject trailing data, unknown fields, or oversized bodies.
- `os.UserHomeDir()` errors are ignored when constructing the tsnet state directory (`cmd/tailrund/main.go:41-43`, `cmd/tailrun-worker/main.go:79-81`).
- Notifications through email, Slack, Discord, webhooks, or Tailscale-friendly integrations.
- Import/export of task definitions and workflow files from Git repositories.


workflow is acyclic directed graph where each node is a command. Workflow is bounded to use some number of cpu cores (using cgroups). We find first worker that has available number of cores and use it.

## graceful worker shutdown / deregistration . Needs persistent state or useless otherwise.
load tasks and workers from db in main.go (or maybe in separe package responsible for persisten state) and give tasks to scheduler package and workers to pool package in New method, them fill stucts with data in New methods.
Workers need to have something unique so worker registration is idempotent
After workers where added to pool in New method they all should be dead and marked alive only if responded to healthcheck, then they can be placed on queue

When shuttingdown, shut down server, wait 
in pool packages drain channels, now tell workers to shutdown and load them to db
scheduler package drain channels, puts executing tasks back in queue (preferably in the front) and loads them to db

need to implement operation for deletin worker
add http endpoint that sends even to pool package
pool package should provide WorkerError method that returns channel
scheduler should listen on that channel and remove task from worker if it gets an error (without release a worker???)
when worker gets an error scheduler checks it's task and puts it in queue (add task log should be ignored for tasks that are in queue)
when pool gets event to delete worker it sens event to WorkerErro channel in pool, send it shutdown request and marks it as shutdown

pool package gets all alive worker and every 5s send them healthcheck request. If worker doesn't respond after n number of times it is marked as dead and event is sent to WorkerError channel to schedule

worker should have unuqie field (each machine) so that registration can be idempotent
