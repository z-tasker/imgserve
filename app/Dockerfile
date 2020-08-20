ARG imgserve_version
FROM mgraskertheband/imgserve:${imgserve_version}

WORKDIR /home/admin/imgserve/app

VOLUME /usr/share/imgserve

ENTRYPOINT ["/bin/bash", "/home/admin/imgserve/app/up.sh"]
