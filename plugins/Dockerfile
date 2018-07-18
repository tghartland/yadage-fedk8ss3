FROM python
COPY yadage-objstore /code/yadage-objstore
RUN cd /code/yadage-objstore && pip install -e . #could become pip install yadage-objstore
COPY datamgmt /code/datamgmt
ENV PYTHONPATH=/code
