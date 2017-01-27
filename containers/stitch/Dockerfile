FROM 218546966473.dkr.ecr.us-east-1.amazonaws.com/stitch-orchestrator:0.0.2

RUN mkdir /code/stream-hubspot
WORKDIR /code
ADD . /code

RUN python3 setup.py install

CMD stitch-orchestrator sync \
  --streamer stream-hubspot \
  --persister persist-stitch \
  --client-id $CLIENT_ID \
--connection-id $CONNECTION_ID
