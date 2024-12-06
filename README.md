# Topics in Cloud Term Research Project - Data Systems for LLMS
Authors: Nikhil Ghosh, Peter McNeely, Matthew Roger Nelson

## ntran arch overview
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
                transactions      to commit
```
[]

## Implementations

[]

## Links
[Design Doc](https://docs.google.com/document/d/1Ep7d3W3R-nh-JVPL33aEnP9De8h6Wffa27PUNAkGWm8/edit?usp=sharing)

[Slack Group](https://app.slack.com/client/T07N57BF9GD/C07N80VR04B)

[System Design](https://miro.com/app/board/uXjVLX50vSk=/)

[November Check-in Slideshow](https://docs.google.com/presentation/d/1zIHVCHv7LhV2vj2HQJPOUswjcYzohjk0gxkfWOCQsEM/edit?usp=sharing)

[Progress/TA Sync Doc](https://docs.google.com/document/d/1ljm8fCeT25hCvfs82JGpiVdmlQjrY5A9XsO8veiTBX8/edit?usp=sharing)
