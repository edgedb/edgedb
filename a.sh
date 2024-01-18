title="GH job to check that postgres/ has not been unintentianally changed"
required_prefix="Update bundled PostgreSQL"

if [[ $title == $required_prefix* ]]; then
  exit 0
fi

git diff --quiet \
  47771cb8d013f2ca15d65f43d518f4f76f4581da \
  712012cc374b54bacfcf13414f23232a2a312e75
if [ $? != 0 ]; then
  echo "postgres/ submodule has been changed,"\
  "but PR title does not indicate that"
  echo "(it should start with '$required_prefix')"
  exit 1
fi