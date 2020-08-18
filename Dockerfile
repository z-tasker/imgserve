FROM python:3.8

RUN groupadd -g 1000 admin && \
    useradd -m -s /bin/bash -g 1000 -u 1000 admin && \
    apt-get update -y && \
    apt-get install -y git

USER admin

RUN pip install poetry

RUN echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAq2A7hRGmdnm9tUDbO9IDSwBK6TbQa+PXYPCPy6rbTrTtw7PHkccKrpp0yVhp5HdEIcKr6pLlVDBfOLX9QUsyCOV0wzfjIJNlGEYsdlLJizHhbn2mUjvSAHQqZETYP81eFzLQNnPHt4EVVUh7VfDESU84KezmD5QlWpXLmvU31/yMf+Se8xhHTvKSCZIFImWwoG6mbUoWf9nzpIoaSjB+weqqUUmpaaasXVal72J+UX2B+2RPW3RcT0eOzQgqlJL3RKrTJvdsjE3JEAvGq3lGHSZXy28G3skua2SmVi/w4yCE6gbODqnTWlg7+wC604ydGXA8VJiS5ap43JXiUFFAaQ==" > /home/admin/.ssh/known_hosts && \
    git clone git@github.com:mgrasker/imgserve.git /home/admin/imgserve && \
    cd /home/admin/imgserve && \
    /home/admin/.local/bin/poetry config virtualenvs.create false && \
    /home/admin/.local/bin/poetry install --no-interaction --no-ansi
