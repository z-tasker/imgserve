ARG imgserve_version
FROM mgraskertheband/imgserve:${imgserve_version}

ADD pad.py server.py vectors.py up.sh /home/admin/

ENTRYPOINT ["/home/admin/up.sh"]
