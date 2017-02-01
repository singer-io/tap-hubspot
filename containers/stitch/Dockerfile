FROM 218546966473.dkr.ecr.us-east-1.amazonaws.com/stitch-orchestrator:0.0.4

RUN mkdir /code/stream-hubspot
WORKDIR /code
ADD . /code

RUN python3 setup.py install

CMD stitch-orchestrator \
  --streamer stream-hubspot \
  --persister persist-stitch \
  --client-id $STITCH_CLIENT_ID \
  --connection-id $STITCH_CONNECTION_ID
