#!bin/bash

ENV="./venv"
if [ -d "$ENV" ];
then
	echo "env found"
	enva
else
	echo "Fresh Setup of env"
	python3 -m venv env
	enva
	python3 -m pip install --upgrade pip
	if [ -f "./requirements.txt" ]; then
		echo "Installing Dependencies"
		pip install -r requirements.txt
	fi
fi
