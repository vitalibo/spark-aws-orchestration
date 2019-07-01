#!/usr/bin/env bash

set -e
cd $(dirname $0)

if [[ $# -ne 1 ]] && [[ $# -ne 2 ]] ;  then
  echo "Usage: $0 [configuration] [profile]"
  echo ''
  echo 'Options:'
  echo '  configuration  The JSON file which contains environment configuration'
  echo '  profile        Use a specific AWS profile from your credential file'
  exit 1
fi

CONFIGURATION="stage/$1.yaml"
PROFILE=`[[ $# -eq 2 ]] && echo $2 || echo 'default'`

function param() {
  yq -r ".Parameters.$1" $CONFIGURATION
}

NAME=`param 'Name'`
ENVIRONMENT=`param 'Environment'`
VERSION=`date -u +%Y%m%dT%H%M%SZ`
BUCKET="s3://`param 'Bucket'`/$NAME/$ENVIRONMENT/$VERSION/"

echo 'Package/Copy artifacts initialized'

mvn clean package -Dmaven.test.skip=true -P $ENVIRONMENT -f ../pom.xml

for MODULE in 'cfn' 'etl'; do
  MODULE="spark-$MODULE-orchestration"
  aws s3 cp "../$MODULE/target/$MODULE-1.0-SNAPSHOT.jar" $BUCKET
done

echo 'Create/Update stack initialized'

function params() {
  yq -r ".$1 | to_entries | map(join(\"=\")) | join(\" \")" $CONFIGURATION
}

aws cloudformation deploy \
  --stack-name "$NAME-infrastructure" \
  --parameter-overrides `params 'Parameters'` Version=$VERSION \
  --capabilities 'CAPABILITY_NAMED_IAM' \
  --tags `params 'Tags'` \
  --profile $PROFILE \
  --template-file stack.yaml