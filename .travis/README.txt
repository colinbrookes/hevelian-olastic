After every regular "git push", artifacts are deployed to oss sonatype snapshot server as snapshots.
If you "git tag 1.1.1" and "git push --tags" afterwards, you will have 1.1.1 version of your library available at maven central.
!Important: Create a separate branch for every release - this will help to do hot fixes in old versions only
