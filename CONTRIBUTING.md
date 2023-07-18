# Contributing

## The project
See [here](https://confluence.esss.lu.se/display/ECDC/Data+Aggregation+and+Streaming) for more details on how the project works.

To summarise:
- For issues that require changes in the functionality of the code (i.e. not documentation changes or typos) create a ticket on the [ESS JIRA board](https://jira.esss.lu.se/secure/RapidBoard.jspa?rapidView=167&view=detail&quickFilter=2154) with the label `FW&FW`.

- These tickets need to be approved by the steering committee before work is started.

## The repository

### Branching

- Branch your feature off 'main'

- Create pull requests against 'main'.

### Branch naming
The names should start with the ticket number and contain a brief description. For example:

`DM-1014_byebye_dead_code`

### Pull requests
There is a template for pull requests. This should contain enough information for the reviewer to be able to review the code efficiently.

## Code

### Style
We use `clang-format` v3.9 LLVM default style.
We also follow the LLVM coding standards for naming conventions etc. and use Doxygen for documentation.

Please refer to [LLVM documentation](https://llvm.org/docs/CodingStandards.html).

### Unit tests
Unit tests should be written/modified for any code added or changed (within reason, of course).

### Integration tests
Integration tests should be written/modified for any changes that affect the "public" API of the application, i.e. anything 
that affects another component of the data streaming pipeline.
