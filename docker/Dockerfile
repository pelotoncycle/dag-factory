ARG AIRFLOW_IMAGE
ARG ACTIONS_RUNNER_VERSION




FROM --platform=linux/amd64 summerwind/actions-runner:${ACTIONS_RUNNER_VERSION}-ubuntu-20.04 as RUNNER
FROM ${AIRFLOW_IMAGE}
USER root 
ARG RUNNER_UID=1000
ARG DOCKER_GID=1001
COPY --from=RUNNER  /usr/bin/entrypoint.sh /usr/bin/startup.sh /usr/bin/logger.sh /usr/bin/graceful-stop.sh /usr/bin/update-status /usr/bin/

RUN type -p curl >/dev/null || sudo apt install curl -y
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
        && sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
        && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
        && sudo apt update \
        && sudo apt install gh -y

# RUN adduser --disabled-password --gecos "" --uid ${RUNNER_UID} runner \
#         && groupadd docker --gid ${DOCKER_GID} \
#         && usermod -aG sudo runner \
#         && usermod -aG docker runner \
#         && echo "%sudo   ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers \
#         && echo "Defaults env_keep += \"DEBIAN_FRONTEND\"" >> /etc/sudoers

USER airflow

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["entrypoint.sh"]