# The purpose of this script is to change some Segments on disk while confirming they change on the server.
# This is just the "client side" and expects you to be running the server already locally.
#!/usr/bin/env bash
set -o errexit
set -o nounset

set -x
# assume we're on linux here
# with tests/resources all loaded, we should see
curl -s localhost:8080/segments | jq '. | length' # 7
curl -s localhost:8080/search/test | jq 'length' # 15

# now let's start messing with stuff, first we'll remove some
rm /tmp/segments/seg_3
# we wait until the polling interval must have passed
# but really the indexer would need to check whether the searchers
# picked up the changes in some cases, eg. if we tried to add
# back the removed segment before the delete is acknowledged
# it'd count as a modify and we'd ignore it
sleep 3
curl -s localhost:8080/segments | jq '. | length' # 6
curl -s localhost:8080/search/test | jq 'length' # 14

# then add them back
cp tests/resources/seg_3 /tmp/segments/seg_3
sleep 3
curl -s localhost:8080/segments | jq '. | length' # 7
curl -s localhost:8080/search/test | jq 'length' # 15
