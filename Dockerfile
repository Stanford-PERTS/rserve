# This base image always uses the specified version of R.
FROM rocker/tidyverse:3.6.2

# Set an environment variable so RServe can detect if it is running within
# a docker container.
ENV RSERVE_DOCKER true

# https://en.wikipedia.org/wiki/Filesystem_Hierarchy_Standard
# https://serverfault.com/questions/124127/linux-fhs-srv-vs-var-where-do-i-put-stuff
WORKDIR /srv/www

# Copy all files from the shell's working directory (where you called
# `docker build`) to the container's working directory (set above).
COPY . .

# Our all-purpose startup script.
CMD ["bash", "docker_init.sh"]

# AppEngine forwards requests to this port
EXPOSE 8080

# Tidbits for later

# delete any data that might exist on a SIGTERM / STOP

# With custom runtimes, client libraries can use the Application Default
# Credentials to authenticate with and call Google APIs.
# https://developers.google.com/identity/protocols/application-default-credentials
