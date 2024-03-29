# create lambda layer directory
mkdir -p lambda_layers/python/lib/python3.9/site-packages

# create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# install required packages
pip3 install --platform manylinux2010_x86_64 \
    --implementation cp --python 3.9 --only-binary=:all: --upgrade \
    --target lambda_layers/python/lib/python3.9/site-packages snowflake-connector-python==2.7.9 requests==2.31.0 toml==0.10.2

# publish the layer
aws lambda publish-layer-version \
    --layer-name snowflake-lambda-layer \
    --compatible-runtimes python3.9 \
    --zip-file fileb://snowflake_lambda_layer.zip