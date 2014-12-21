#!/bin/bash
exists () {
    type "$1" >/dev/null 2>/dev/null
}

if ! exists brew && [ "$(uname)" == 'Darwin' ]; then
    echo "installing homebrew"
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
fi
if ! exists spark-submit && [ "$(uname)" == 'Darwin' ]; then
    echo "installing spark via homebrew"
    brew install apache-spark
fi

# run spark program locally
spark-submit --master local[4] sentiment.py $1
