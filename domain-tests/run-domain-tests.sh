#!/bin/sh

scl enable rh-python38 -- ~/venv/bin/pytest --file-maker-binary=../kafka-to-nexus --junitxml=DomainTestsOutput.xml
