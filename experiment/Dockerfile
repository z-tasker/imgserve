ARG imgserve_version
FROM mgraskertheband/imgserve:${imgserve_version}

ADD jzazbz_array.npy /home/admin/imgserve/jzazbz_array.npy

WORKDIR /home/admin/imgserve

USER root
RUN curl -sSL https://get.docker.com/ | sh && \
    usermod -aG docker admin
USER admin
RUN git pull

ENTRYPOINT ["/bin/bash", "/home/admin/imgserve/experiment.sh"]
