ARG imgserve_version
FROM mgraskertheband/imgserve:${imgserve_version}

ADD jzazbz_array.npy /home/admin/imgserve/jzazbz_array.npy

WORKDIR /home/admin/imgserve

RUN git pull

ENTRYPOINT ["/bin/bash", "/home/admin/imgserve/experiment.sh"]
