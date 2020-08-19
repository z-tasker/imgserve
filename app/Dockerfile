ARG imgserve_version
FROM mgraskertheband/imgserve:${imgserve_version}

WORKDIR /home/admin/imgserve/app

RUN mkdir /usr/share/imgserve

ENTRYPOINT ["/bin/bash", "/home/admin/imgserve/app/up.sh"]
