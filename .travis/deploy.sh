#!/usr/bin/env bash

set -e

# only do deployment, when travis detects a new tag
if [ ! -z "$TRAVIS_TAG" ]
then
    echo "on a tag -> set pom.xml <version> to $TRAVIS_TAG"
    mvn --settings .travis/settings.xml org.codehaus.mojo:versions-maven-plugin:2.3:set -DnewVersion=$TRAVIS_TAG -Prelease

    if [ ! -z "$TRAVIS" -a -f "$HOME/.gnupg" ]; then
        shred -v ~/.gnupg/*
        rm -rf ~/.gnupg
    fi

    source .travis/gpg.sh

else
    echo "not on a tag -> keep snapshot version in pom.xml"
fi
mvn clean deploy --settings .travis/settings.xml -DskipTests=true --batch-mode --update-snapshots
if [ ! -z "$TRAVIS" ]; then
        shred -v ~/.gnupg/*
        rm -rf ~/.gnupg
    fi