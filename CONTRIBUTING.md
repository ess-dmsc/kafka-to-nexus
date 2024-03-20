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

`ECDC-1014_byebye_dead_code`

### Pull requests
There is a template for pull requests. This should contain enough information for the reviewer to be able to review the code efficiently.

## Code

### Style
We use `clang-format` v3.9 LLVM default style.
We try to follow the [C++ Core Guidelines](https://isocpp.github.io/CppCoreGuidelines/) where possible.
We currently use Doxygen for documentation, but try to keep it as simple as possible.

We used to follow the [LLVM Coding Standards](https://llvm.org/docs/CodingStandards.html) for naming conventions but in 
practice it was found to be difficult to read, so we are switching to a style similar to Python, e.g.,:
- lower snake-case for variable and function names
- capitalised words for class names

Private class members should have a trailing underscore.

As we are migrating the style, one will see code in both styles. We suggest just fixing the style for code local to your
current work. Eventually, we will eliminate the LLVM style.

### Unit tests
Unit tests should be written/modified for any code added or changed (within reason, of course).

### Integration tests
Integration tests should be written/modified for any changes that affect the "public" API of the application, i.e. anything 
that affects another component of the data streaming pipeline.
