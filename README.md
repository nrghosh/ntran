# Topics in Cloud Term Research Project - Data Systems for LLMS
Authors: Nikhil Ghosh, Peter McNeely, Matthew Roger Nelson

## ntran Architectural Overview
```
ntran/
│
├── main.go         (Entry point, handles CLI)
│                   ┌──────────────────┐
├── policy/         │ Policy Interface │ 
│   │               └──────────────────┘
│   ├── policy.go   (Defines interface for all implementations)
│   │    
│   ├── Implementations ──────────────────────┐
│   │   ├── serialclient.go     (PostgreSQL)  │
│   │   ├── duckdbparallelclient.go (DuckDB)  │ Share common test cases
│   │   ├── duckdbserialclient.go (DuckDB)    │ and benchmarking logic
│   │   ├── coldneondbclient.go (NeonDB)      │ 
│   │   └── prewarmneondbclient.go (NeonDB)   ┘
│   │
│   ├── queries.go   (Shared SQL test cases)
│   ├── benchmark.go (Performance measurement)
│   └── experiment.go (Results collection)
│
├── schemas/        (Database schemas)
│   ├── schema.sql    (Initial setup)
│   └── rollback.sql  (Cleanup)
│
└── results/        (Benchmark outputs)
    └── *.csv       (Raw timing data)

Flow:
 ┌─────────┐    ┌──────────┐    ┌───────────────┐    ┌──────────┐
 │  Setup  │ -> │ Execute  │ -> │ Random Winner │ -> │ Cleanup  │
 └─────────┘    └──────────┘    └───────────────┘    └──────────┘
     │              │                   │                 │
 schema.sql     N parallel        Pick 1 result      rollback.sql
                or serial         to commit
                transactions      
```

## Building ntran
ntran depends on golang 1.23.2, so be sure to get that (https://go.dev/dl/). Then navigate to the ntran directory (i.e. `cd ntran`).
To build the executable, simply run `go build`. This will generate an `ntran` executable.

## Running ntran
You can run the ntran executable built above, or from within the ntran directory, you can run ntran by executing the command `go run .`.
This section will assume you have built the executable (but if you simply just replace `./ntran` with `go run .`, it should work equivalently).

To run an experiment, you must specify the policy to execute. For instance, to run the `serial-snapshot` experiment, run `./ntran -policy serial-snapshot`. Run `./ntran --help` to view the full usage and entire list of supported policies.

## Supported Policies
### serial-snapshot
This policy executes N transactions sequentially under one parent transaction on a postgres database. After each sub-transaction has performed its command, the sub-transaction is rolled back.

This policy expects to connect to a Postgres database whose connection string can be found under the environment variable `SERIAL_DATABASE_URL`.

### duckdb-parallel
This policy executes N transactions on N instances of DuckDB in parallel with one another.

### duckdb-serial
This policy executes N transactions on N instances of DuckDB in sequence with one another.

### cold-neondb
This policy executes N transactions on N instances of NeonDB in parallel with one another. The "cold" in cold-neondb is a reference to the fact that compute nodes are spun up right before a given transaction is executed.

This policy depends on the `neon` cli tool for branch management commands. To download, follow the instructions here: https://neon.tech/docs/reference/neon-cli. Don't call the CLI `neonctl` like the docs suggest. We expect it to be called `neon`.

The NeonDB project used by the authors is https://console.neon.tech/app/projects/patient-hall-76729406.

### prewarm-neondb
This policy executes N transactions on N instances of NeonDB in parallel with one another. The "prewarm" in prewarm-neondb is a reference to the fact that N compute nodes are spun up prior to any transaction being executed.

This policy depends on the `neon` cli tool for branch management commands. To download, follow the instructions here: https://neon.tech/docs/reference/neon-cli. Don't call the CLI `neonctl` like the docs suggest. We expect it to be called `neon`.

The NeonDB project used by the authors is https://console.neon.tech/app/projects/patient-hall-76729406.

## Links
[Design Doc](https://docs.google.com/document/d/1Ep7d3W3R-nh-JVPL33aEnP9De8h6Wffa27PUNAkGWm8/edit?usp=sharing)

[Slack Group](https://app.slack.com/client/T07N57BF9GD/C07N80VR04B)

[System Design](https://miro.com/app/board/uXjVLX50vSk=/)

[November Check-in Slideshow](https://docs.google.com/presentation/d/1zIHVCHv7LhV2vj2HQJPOUswjcYzohjk0gxkfWOCQsEM/edit?usp=sharing)

[Progress/TA Sync Doc](https://docs.google.com/document/d/1ljm8fCeT25hCvfs82JGpiVdmlQjrY5A9XsO8veiTBX8/edit?usp=sharing)
