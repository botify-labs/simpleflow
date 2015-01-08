#!/bin/bash

echo -n "GitHub User: "
read USER

echo -n "GitHub Password: "
read -s PASS

echo ""
echo -n "GitHub Repo (e.g. foo/bar): "
read REPO

REPO_USER=$(echo "$REPO" | cut -f1 -d /)
REPO_NAME=$(echo "$REPO" | cut -f2 -d /)
REPO_URL="https://api.github.com/repos/"$REPO_USER"/"$REPO_NAME"/labels"

declare -a arrayGithubLabels=(bug duplicate enhancement help+wanted invalid question wontfix)

#echo "exemple"
#curl --user "$USER:$PASS" --include --request DELETE "https://api.github.com/repos/$REPO_USER/$REPO_NAME/labels/bug"

for labelName in "${arrayGithubLabels[@]}"
do
  >&2 echo "deleting label:" $REPO_URL/$labelName
  curl --user "$USER:$PASS" --include --request DELETE $REPO_URL/$labelName
done

# create labels
declare -A arrayLabels
arrayLabels["Bugfest"]="e11d21"
arrayLabels["customer"]="fbca04"
arrayLabels["done: canceled"]="009800"
arrayLabels["done: duplicate"]="009800"
arrayLabels["done: fixed"]="009800"
arrayLabels["done: invalid"]="009800"
arrayLabels["done: moved"]="009800"
arrayLabels["effort: important"]="fef2c0"
arrayLabels["effort: medium"]="fef2c0"
arrayLabels["effort: small"]="fef2c0"
arrayLabels["on hold"]="e11d21"
arrayLabels["status: 1-backlog"]="d4c5f9"
arrayLabels["status: 2-this week"]="d4c5f9"
arrayLabels["status: 3-development"]="5319e7"
arrayLabels["status: 4-help wanted"]="eb6420"
arrayLabels["status: 5-waiting for review"]="5319e7"
arrayLabels["status: 6-ready for staging"]="5319e7"
arrayLabels["status: 7-testing"]="5319e7"
arrayLabels["status: 8-ready for prod"]="5319e7"
arrayLabels["type: brainstorm"]="207de5"
arrayLabels["type: bug"]="e11d21"
arrayLabels["type: enhancement"]="207de5"
arrayLabels["type: feature"]="207de5"
arrayLabels["type: hotfix"]="e11d21"
arrayLabels["type: subfeature"]="207de5"
arrayLabels["type: task"]="207de5"
arrayLabels["urgent"]="eb6420"

#echo "exemple"
#curl -v -u nathG --include --request POST --data '{"name":"acce pted","color":"66aa00"}' "https://api.github.com/repos/botify-labs/chaplin/labels"

for labelName in "${!arrayLabels[@]}"
do
  labelColor=${arrayLabels[$labelName]}
  labelData='{"name":"'$labelName'","color":"'$labelColor'"}'
  >&2 echo "adding label:" $labelData
  curl --user "$USER:$PASS" --include --request POST --data "$labelData" $REPO_URL
done