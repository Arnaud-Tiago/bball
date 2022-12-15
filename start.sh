#!/bin/bash
cd /home/arnaud/bball && pwd && export $(grep -v '^#' .env | xargs) && python main.py
exit
