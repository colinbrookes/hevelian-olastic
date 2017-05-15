#!/usr/bin/env bash

set -ev
# do deployment using release profile, when travis detects a new tag
if [ ! -z "$TRAVIS_TAG" ]
then
    echo "on a tag -> set pom.xml <version> to $TRAVIS_TAG"
    mvn --settings .travis/settings.xml org.codehaus.mojo:versions-maven-plugin:2.3:set -DnewVersion=$TRAVIS_TAG -Prelease

    mvn clean deploy --settings .travis/settings.xml -DskipTests=true --batch-mode --update-snapshots -Prelease
# do deployment to snapshot repo
else
    echo "not on a tag -> keep snapshot version in pom.xml"
    mvn clean deploy --settings .travis/settings.xml -DskipTests=true --batch-mode --update-snapshots
fi
