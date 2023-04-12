# Run this script to re-build the graphiql.min.js and graphiql.min.css files

if [ ! -d ./graphiql ]
then
  git clone https://github.com/edgedb/graphiql.git \
    --branch globals-editor --depth=1
else
  cd ./graphiql
  git checkout globals-editor && git pull
  cd ../
fi

cd ./graphiql
yarn
yarn build
yarn workspace graphiql build-bundles-min
cd ../

cp ./graphiql/packages/graphiql/graphiql.min.js ./graphiql.min.js
cp ./graphiql/packages/graphiql/graphiql.min.css ./graphiql.min.css
