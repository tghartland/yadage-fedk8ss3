__ACCESSKEY__=... __SECRETKEY__=... __HOST__=s3.cern.ch __BUCKET__=testbucket envsubst < runspec.yml.tmpl > spec.yml

< source yadage and plugins > 
yadage-run -f spec.yml             

