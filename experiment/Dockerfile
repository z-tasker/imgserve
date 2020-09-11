ARG imgserve_version
FROM mgraskertheband/imgserve:${imgserve_version}

ADD jzazbz_array.npy /home/admin/imgserve/jzazbz_array.npy

WORKDIR /home/admin/imgserve

USER root
RUN curl -sSL https://get.docker.com/ | sh && \
    usermod -aG docker admin

RUN apt-get -y update && \
    apt-get -y install python3-opencv && \
    wget -O "/home/admin/imgserve/haarcascade_frontalface_alt.xml" "https://raw.githubusercontent.com/opencv/opencv/master/data/haarcascades/haarcascade_frontalface_alt.xml"

USER admin
RUN git pull

ENTRYPOINT ["/bin/bash", "/home/admin/imgserve/experiment.sh"]
