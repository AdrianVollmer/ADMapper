#!/bin/bash

cloc --fullpath --not-match-d='.*vendor.*' --vcs=git --exclude-ext=json
